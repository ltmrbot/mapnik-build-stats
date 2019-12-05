import os
import re
import shlex
import subprocess
import yaml
from hashlib import sha1 as COMPILER_INPUT_HASHER
from procutil import check_exit, popen, popen2, strdatetime
from sys import intern
from time import time


def command_output_hash(*args, **kwds):
    proc = popen(*args, stdout=subprocess.PIPE, **kwds)
    hasher = COMPILER_INPUT_HASHER()
    chunk = bytearray(8192)
    while True:
        n = proc.stdout.readinto(chunk)
        if n < len(chunk):
            hasher.update(memoryview(chunk)[:n])
            break
        hasher.update(chunk)
    return None if proc.wait() else intern(hasher.hexdigest())


def filtered_args_hash(args):
    relevant = []
    skipnext = False
    for arg in args:
        if skipnext:
            skipnext = False
            continue
        # skip preprocessor options (-D, -U),
        # include path options (-I et al.),
        # and output file option (-o)
        m = re.match('-(D|U|I|imacros|include|iquote|isystem|o)', arg)
        if m:
            skipnext = (arg == m.group())
        else:
            relevant.append(shlex.quote(arg))
    hasher = COMPILER_INPUT_HASHER()
    hasher.update(' '.join(relevant).encode('UTF-8'))
    return intern(hasher.hexdigest())


class CommitInfo(object):

    def __init__(self, sha1, cdate, subject):
        self.sha1 = sha1
        self.cdate = cdate
        self.subject = subject
        self.data = {'commit_date': cdate, 'commit_subject': subject}
        self.updated = False
        self.gap_before = 7305 * 24 * 3600
        self.gap_after = 14610 * 24 * 3600
        self._sources = None

    def __str__(self):
        return F'commit {self.sha1} {strdatetime(self.cdate)} "{self.subject}"'

    def _data_dir(self, data_root):
        return os.path.join(data_root, 'commits', self.sha1[0], self.sha1[1])

    def _metadata_yml(self, data_root):
        return os.path.join(self._data_dir(data_root), self.sha1 + '-metadata.yml')

    def _sources_yml(self, data_root):
        return os.path.join(self._data_dir(data_root), self.sha1 + '-sources.yml')

    def load_metadata(self, *, data_root='data', Loader=yaml.SafeLoader):
        try:
            with open(self._metadata_yml(data_root), 'r') as fr:
                data = yaml.load(fr, Loader)
            if isinstance(data, dict):
                self.data = data
                self.updated = False
                return True
            return False
        except Exception:
            return False

    def load_sources(self, *, data_root='data', Loader=yaml.SafeLoader):
        try:
            with open(self._sources_yml(data_root), 'r') as fr:
                sources = yaml.load(fr, Loader)
            if isinstance(sources, dict):
                self._sources = sources
                return True
            return False
        except Exception:
            return False

    def metadata(self):
        return self.data

    def save_data(self, *, data_root='data'):
        if not self.updated:
            return
        os.makedirs(self._data_dir(data_root), exist_ok=True)
        if self.data.get('configure_ok'):
            with open(self._sources_yml(data_root), 'w') as fw:
                yaml.safe_dump(self._sources, stream=fw, default_flow_style=False)
        else:
            try:
                os.unlink(self._sources_yml(data_root))
            except OSError:
                pass
        with open(self._metadata_yml(data_root), 'w') as fw:
            yaml.safe_dump(self.data, stream=fw, default_flow_style=False)

    def sortkey(self):
        return self.gap_before + self.gap_after

    def sources(self):
        return self._sources

    def update_sources(self, repo, targets):
        repo.clean()
        repo.checkout(self)
        exit_code = repo.configure()
        if exit_code:
            self.data['configure_ok'] = False
            sources = None
        else:
            self.data['configure_ok'] = True
            sources = {}
            for line in repo.get_build_commands(*targets):
                args = shlex.split(line)
                try:
                    srcfile = args[-1]
                    if not srcfile.endswith('.cpp'):
                        continue
                    ic = args.index('-c')
                    io = args.index('-o')
                    cpp_args = args.copy()
                    cpp_args[ic] = '-E'
                    cpp_args[io + 1] = '-'
                    quoted_args = list(map(shlex.quote, args))
                    quoted_args[io + 1] = '${TARGET}'
                    quoted_args[-1] = '${SOURCE}'
                except (IndexError, ValueError):
                    continue
                srcfile = intern(srcfile)
                cpp_hash = command_output_hash(cpp_args, cwd=repo.dir)
                arg_hash = filtered_args_hash(args[:-1])
                sources[srcfile] = {'compiler_args': ' '.join(quoted_args),
                                    'filtered_args_hash': arg_hash,
                                    'preprocessed_hash': cpp_hash}
                if len(sources) % 75 == 0:
                    print(F'\npreprocessed {len(sources)} sources')
            if len(sources) % 75 != 0:
                print(F'\npreprocessed {len(sources)} sources')
        self._sources = sources
        self.data['commit_date'] = self.cdate
        self.data['commit_subject'] = self.subject
        self.data['last_refresh'] = int(time())
        self.data['targets'] = sorted(targets)
        self.updated = True

#endclass


class GitRepo(object):

    def __init__(self, src_url, dst_dir, *, clone_args=()):
        self.url = src_url
        self.dir = dst_dir
        if src_url is None:
            return
        proc = popen('git', 'clone', '--no-local', '--no-checkout',
                                     '--single-branch', '--depth=100',
                                     clone_args, '--', src_url, dst_dir,
                     highlight=[0,1])
        proc.wait()

    def _fetch_branch(self, branch, since):
        bremote = F'origin/{branch}'
        self.git('config', '--add', 'remote.origin.fetch',
                 F'+refs/heads/{branch}:refs/remotes/{bremote}')
        older = ''
        while older == '':
            self.git('fetch', '--deepen', 100, 'origin', branch)
            revs = self.git_rev_list('-1', '--first-parent',
                                     '--until', since, bremote, '--')
            older = max((line.strip() for line in revs), default='')
        return bremote

    def checkout(self, commit):
        self.git('checkout', '--force', commit.sha1, '--')

    def clean(self):
        self.git('clean', '-dffqx', '--')

    def configure(self):
        proc = popen('./configure', cwd=self.dir)
        return proc.wait()

    def git(self, *args, **kwds):
        proc = popen('git', '-C', self.dir, *args,
                     highlight=[0,3], **kwds)
        check_exit(proc)

    def git_log(self, *args, **kwds):
        return self.capture_git('log', *args, **kwds)

    def git_rev_list(self, *args, **kwds):
        return self.capture_git('rev-list', *args, **kwds)

    def capture_git(self, *args, **kwds):
        proc = popen2('git', '-C', self.dir, *args,
                      highlight=[0,3], **kwds)
        yield from proc.stdout
        check_exit(proc)

    def tip_sha1(self, *args, **kwds):
        proc = popen2('git', '-C', self.dir,
                      'rev-parse', '--verify', '--default', 'HEAD',
                      *args, highlight=[0,3], **kwds)
        out, err = proc.communicate()
        return out.rstrip('\n')

    def commits_since(self, since, heads):
        return list(self.iter_commits_since(since, heads))

    def iter_commits_since(self, since, heads):
        rheads = tuple(self._fetch_branch(b, since) for b in heads)
        log1 = self.git_log('--first-parent', '--format=%H', '-F', '-i',
                            '--grep=[skip ci]', '--grep=[skip travis]',
                            '--since', since, rheads, '--')
        log2 = self.git_log('--first-parent', '--format=%H %ct %s',
                            '--since', since, rheads, '--')
        # read log1 output while log2 is running in parallel
        skipped = set(line.strip() for line in log1)
        for line in log2:
            #print('got line', line.strip())
            try:
                c_hash, c_time, c_subj = line.strip().split(maxsplit=2)
                c_time = int(c_time)
            except ValueError:
                continue
            if c_hash in skipped:
                #print('skipping', c_hash)
                continue
            yield CommitInfo(c_hash, c_time, c_subj)

    def timed_command(self, *args):
        proc = popen2('/usr/bin/time', '-o', '/dev/stdout',
                      '-f', '%U %M %F', '--quiet', *args,
                      stderr=subprocess.DEVNULL,
                      highlight=[0,6], cwd=self.dir)
        lastline = ''
        for line in proc.stdout:
            line = line.strip()
            if line:
                lastline = line
        lastline = lastline.split()
        try:
            res = dict(duration=float(lastline[0]),
                       memory=int(lastline[1]),
                       pagefaults=int(lastline[2]),
                       timestamp=int(time()))
        except (IndexError, ValueError):
            res = dict(failed=True)
        if proc.wait():
            res['failed'] = True
        return res

#endclass
