import os
import re
import shlex
import yaml
from asyncutil import async_batch
from hashlib import sha1 as COMPILER_INPUT_HASHER
from procutil import check_exit, popen, popen2, strdatetime
from subprocess import CalledProcessError, DEVNULL, PIPE
from sys import intern
from time import time


async def command_output_hash(*args, **kwds):
    from asyncutil import StreamHasher
    from procutil import async_popen
    hasher = StreamHasher(COMPILER_INPUT_HASHER)
    proc = await async_popen(*args, stdin=DEVNULL, stderr=DEVNULL,
                             stdout_filter=hasher, **kwds)
    if await proc.wait() != 0:
        raise SystemExit(proc.returncode)
    return await hasher.hexdigest()


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
        self._fixed = {'commit_date': cdate, 'commit_subject': subject}
        self.data = self._fixed.copy()
        self.updated = False
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
            data.update(self._fixed)
            self.data.clear()
            self.data.update(data)
            self.updated = False
            return True
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

    def sources(self):
        return self._sources

    async def update_sources(self, repo, targets):
        exit_code = repo.checkout_and_configure(self)
        if exit_code:
            self.data['configure_ok'] = False
            self._sources = None
        else:
            self.data['configure_ok'] = True
            self._sources = sources = {}
            async for sfile, smeta in repo.preprocess_sources(*targets):
                sources[sfile] = smeta
        self.data['commit_date'] = self.cdate
        self.data['commit_subject'] = self.subject
        self.data['last_refresh'] = int(time())
        self.data['targets'] = sorted(targets)
        self.updated = True
        return exit_code

#endclass


class GitRepo(object):

    def __init__(self, src_url, dst_dir, *, clone_args=()):
        self.url = src_url
        self.dir = dst_dir
        if src_url is None:
            return
        proc = popen('git', 'clone', '--no-checkout',
                                     '--no-local',
                                     '--single-branch',
                                     '--depth=200',
                                     clone_args, '--', src_url, dst_dir,
                     highlight=[0,1])
        proc.wait()

    def checkout(self, commit):
        self.git('checkout', '--force', commit.sha1, '--')

    def checkout_and_configure(self, commit):
        if self.tip_sha1() != commit.sha1:
            self.clean()
            self.checkout(commit)
        return self.configure()

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
        return out.rstrip('\r\n')

    def fetch_branches(self, since, *heads):
        refs = tuple(F'refs/heads/{branch}:refs/remotes/origin/{branch}'
                     for branch in heads)
        self.git('fetch', '--shallow-since', since, 'origin', *refs)

    def commits_since(self, since, *heads):
        return list(self.iter_commits_since(since, *heads))

    def iter_commits_since(self, since, *heads):
        refs = tuple(F'origin/{branch}' for branch in heads)
        log1 = self.git_log('--first-parent', '--format=%H', '-F', '-i',
                            '--grep=[skip ci]', '--grep=[skip travis]',
                            '--since', since, refs, '--')
        log2 = self.git_log('--first-parent', '--format=%H %ct %s',
                            '--since', since, refs, '--')
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

    async def timed_command(self, *args):
        from procutil import async_popen
        proc = await async_popen(
                '/usr/bin/time', '-o', '/dev/stdout', '-f', '%U %M %F', '--quiet',
                *args, highlight=[0,6], cwd=self.dir,
                stdin=DEVNULL, stdout=PIPE, stderr=DEVNULL)
        lastline = b''
        async for bline in proc.stdout:
            bline = bline.strip()
            if bline:
                lastline = bline
        lastline = lastline.split()
        try:
            res = dict(duration=float(lastline[0]),
                       memory=int(lastline[1]),
                       pagefaults=int(lastline[2]),
                       timestamp=int(time()))
        except (IndexError, ValueError):
            res = {'failed': 'parsing /usr/bin/time output'}
        if await proc.wait() != 0:
            res.setdefault('failed', proc.returncode or True)
        return res

    async def preprocess_one(self, cxx_args, cpp_args):
        srcfile = intern(cxx_args.pop())
        arg_hash = filtered_args_hash(cxx_args)
        cpp_hash = await command_output_hash(*cpp_args, cwd=self.dir)
        return srcfile, {'compiler_args': ' '.join(cxx_args),
                         'filtered_args_hash': arg_hash,
                         'preprocessed_hash': cpp_hash}

    async def _aiter_cmdlines(self, *targets):
        try:
            async for cmdline in self.get_build_commands(*targets):
                yield cmdline
        except CalledProcessError as ex:
            print("non-fatal: subprocess returned", ex.returncode)
            print("failed command:", *map(shlex.quote, ex.cmd))

    async def _aiter_preprocess(self, *targets):
        async for cmdline in self._aiter_cmdlines(*targets):
            cxx_args = shlex.split(cmdline)
            try:
                srcfile = cxx_args[-1]
                if not srcfile.endswith('.cpp'):
                    continue
                io = cxx_args.index('-o')
                del cxx_args[io:io + 2]
                ic = cxx_args.index('-c')
                cpp_args = cxx_args.copy()
                cpp_args[ic] = '-E'
            except (IndexError, ValueError):
                continue
            # yield coroutine object, no await
            yield self.preprocess_one(cxx_args, cpp_args)

    async def preprocess_sources(self, *targets):
        count = 0
        aiter = self._aiter_preprocess(*targets)
        # preprocessor doesn't require huge amounts of memory,
        # so we can run a couple of those in parallel
        async for sfile, smeta in async_batch(aiter, max_concurrent=2):
            yield sfile, smeta
            count += 1
            if count % 75 == 0:
                print(F'\npreprocessed {count} sources')
        if count % 75 != 0:
            print(F'\npreprocessed {count} sources')

#endclass
