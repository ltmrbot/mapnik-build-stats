#! /usr/bin/env python3

import argparse
import os
import procutil
import random
import shlex
import shutil
import tempfile
from time import time
from datacache import DataCache
from gitutil import GitRepo
from procutil import check_exit, popen, popen2, strdatetime


class DeadlineReached(BaseException):
    """
    Inherits from BaseException instead of Exception so that it is not
    accidentally caught by code that catches Exception.

    """

    def __init__(self, deadline):
        super().__init__(deadline)
        self.deadline = deadline

#endclass


class ArgumentNamespace(argparse.Namespace):

    def check_deadline(self):
        deadline = self.deadline
        if deadline is not None and deadline <= time():
            raise DeadlineReached(deadline)

    @property
    def deadline_reached(self):
        deadline = self.deadline
        if deadline is None:
            return False
        else:
            return deadline <= time()

#endclass


class MapnikGitRepo(GitRepo):

    def checkout(self, commit):
        self.git('checkout', '--force', commit.sha1, '--')
        if commit.cdate < 1511519354: # 2017-11-24 11:29:14 +0100
            self.python = 'python2'
        else:
            self.python = 'python3'
            # 8cd2ae322 2017-11-22 14:56:20 +0100 "support both python 2 and 3."
            # b0f3f1aed 2017-11-24 11:29:14 +0100 "port Scons3 support from master"

        if ARGS.use_local_submodules:
            self.setup_local_submodules()

        self.git('submodule', 'update', '--init', '--', 'deps/')

    def clean(self):
        self.git('clean', '-dffqx',
                          '--exclude=mason_packages/.binaries',
                          '--exclude=mason_packages/.cache')

    def bootstrap(self):
        stderr = None
        nok = getattr(self, 'bootstrap_ok', 0)
        if nok >= 5:
            from subprocess import DEVNULL as stderr
            print(F'bootstrap.sh output squelched after {nok} successful runs')
        check_exit(popen('bash', 'bootstrap.sh',
                         highlight=[0,1], cwd=self.dir, stderr=stderr))
        self.bootstrap_ok = nok + 1

    def configure(self):
        if ARGS.use_mason:
            self.bootstrap()
        configure_script = F'''
source ./mapnik-settings.env || true
{shlex.quote(self.python)} scons/scons.py --implicit-deps-changed \\
    configure \
CC={shlex.quote(os.environ.get("CC", "cc"))} \
CXX={shlex.quote(os.environ.get("CXX", "c++"))}
'''
        proc = popen('bash', '-c', configure_script, 'configure.bash',
                     highlight=[0,2], cwd=self.dir)
        with open(os.path.join(self.dir, 'config.ok_for_commit'), 'w') as fw:
            if proc.wait() == 0:
                self.git('rev-parse', 'HEAD', stdout=fw)
        return proc.wait()

    def scons(self, *args, **kwds):
        return popen(self.python, 'scons/scons.py', *args,
                     highlight=[0,1], cwd=self.dir, **kwds)

    def get_build_commands(self, *targets):
        print('Generating build commands...', *targets)
        proc = popen2(self.python, 'scons/scons.py',
                      '--dry-run', '--no-cache', *targets,
                      cwd=self.dir)
        yield from proc.stdout
        check_exit(proc)

    def setup_local_submodules(self):
        import re
        src_repo = os.path.abspath(self.url)
        entries = self.capture_git('config', '-f', '.gitmodules', '--list', '--name-only')
        for line in entries:
            m = re.match(R'^submodule\.(.*)\.url$', line)
            if m:
                cfg_key = m.group()
                cfg_url = os.path.join(src_repo, '.git', 'modules', m.group(1))
                self.git('config', cfg_key, cfg_url)

#endclass


def next_compile_threshold(base_delay_hours, compile_timestamps):
    n = len(compile_timestamps)
    latest = compile_timestamps[-1]
    multiplier = 3600 * (1.5 * n - 0.5 / n)
    return latest + base_delay_hours * multiplier
#enddef


def consider_commit(c, repo, dcache):
    try:
        sources = dcache.require_commit_sources(c, repo)
    finally:
        dcache.save_commit_data(c)

    ARGS.check_deadline()
    if sources is None:
        vprint('skipping because configure failed at'
               F' {strdatetime(dcache.last_commit_refresh(c))}')
        return False

    c.comp_keys = {}
    comp_tss = []
    for src_path, s in sources.items():
        cpp_hash = s.get('preprocessed_hash')
        arg_hash = s.get('filtered_args_hash')
        if cpp_hash is None:
            continue
        ts = dcache.require_compile_timestamps(src_path, arg_hash, cpp_hash)
        comp_tss.append(ts)
        c.comp_keys[src_path] = (cpp_hash, arg_hash, ts)

    full_builds = min(map(len, comp_tss), default=0)
    least_recent_last = min((ts[-1] for ts in comp_tss if ts), default=None)
    most_recent_last = max((ts[-1] for ts in comp_tss if ts), default=None)
    now = time()
    if full_builds:
        if all(now < next_compile_threshold(13, ts) for ts in comp_tss):
            vprint('skipping because all sources compiled between'
                   F' {strdatetime(least_recent_last)}'
                   F' and {strdatetime(most_recent_last)}')
            return False
    ARGS.check_deadline()
    print(F'\nfull_builds={full_builds} last compiled between'
          F' {strdatetime(least_recent_last) if least_recent_last else "NEVER"}'
          F' and {strdatetime(most_recent_last) if most_recent_last else "NEVER"}')
    return True


def process_commit(c, repo, dcache):
    now = time()
    tuples = []
    for src_path, (cpp_hash, arg_hash, ts) in c.comp_keys.items():
        if ts:
            if now < next_compile_threshold(11, ts):
                continue
            t_last = ts[-1]
        else:
            t_last = 0
        tuples.append((t_last, src_path, arg_hash, cpp_hash))
    if not tuples:
        return
    try:
        with open(os.path.join(repo.dir, 'config.ok_for_commit'), 'r') as fr:
            ok_sha1 = fr.read().strip()
        configured = (ok_sha1 == c.sha1 == repo.tip_sha1())
    except:
        configured = False
    if not configured:
        repo.clean()
        repo.checkout(c)
        exit_code = repo.configure()
        if exit_code:
            return
    num_done = 0
    tuples.sort()
    for t_last, src_path, arg_hash, cpp_hash in tuples:
        ARGS.check_deadline()
        args = shlex.split(c.sources()[src_path]['compiler_args'])
        args[args.index('${SOURCE}')] = src_path
        args[args.index('${TARGET}')] = src_path + '.o'
        cr = repo.timed_command(*args)
        sdata = dcache.require_source_data(src_path, arg_hash)
        crs = sdata.setdefault(cpp_hash, [])
        crs.append(cr)
        if len(crs) > ARGS.max_samples:
            crs.sort(key=lambda cr: cr['timestamp'])
            del crs[:-ARGS.max_samples]
        dcache.save_source_data(src_path, arg_hash)
        num_done += 1
        if num_done % 75 == 0 or num_done == len(tuples):
            # ignore ARGS.verbose:
            # - in non-verbose mode, we need to split long lines of dots
            # - in verbose mode, an extra line won't hurt
            print(F'\ncompiled {num_done}/{len(tuples)} sources from {c}')


def vprint(*args, **kwds):
    if ARGS.verbose:
        print(*args, **kwds)


def _main():
    procutil.verbose = ARGS.verbose

    tmp = os.path.join(tempfile.gettempdir(), 'build-stats')
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp, exist_ok=False)

    cachedir = os.path.join(tempfile.gettempdir(), 'build-stats-cache')
    os.makedirs(cachedir, exist_ok=True)

    start = time()
    repo = MapnikGitRepo(ARGS.source_repository, os.path.join(tmp, 'mapnik'))
    commits = repo.commits_since(ARGS.since, ARGS.branches)

    print(F'\nFound {len(commits)} commits')
    if not commits:
        return 0

    data_repo = GitRepo(None, ARGS.data_dir)
    dcache = DataCache(cachedir,
                       data_repo=data_repo,
                       #targets=('deps/',))
                       targets=('src/', 'deps/', 'plugins/', 'utils/'))

    try:
        n_commits = len(commits)
        while commits:
            ARGS.check_deadline()
            # Pick a commit randomly, favouring more recent ones.
            i = int(len(commits) * random.random() ** 3)
            #        \              \
            #         \              X uniform [0; 1)
            #          Y = n * (X ** 3)
            #          CDF_Y(n / 8) ~ 50%
            c = commits.pop(i)
            print(F' {n_commits - len(commits)}/{n_commits} checking {c}')
            if consider_commit(c, repo, dcache):
                process_commit(c, repo, dcache)
    except DeadlineReached as ex:
        print(F'\nreached deadline {strdatetime(ex.deadline)}')

    msg = (F'N={ARGS.max_samples} since={ARGS.since}'
           F' branches={",".join(ARGS.branches)}')
    try:
        msg = (F'travis {os.environ["TRAVIS_JOB_NUMBER"]} {msg}\n\n'
               F'{os.environ["TRAVIS_BUILD_WEB_URL"]}')
    except KeyError:
        msg = F'run {strdatetime(start)} {msg}'

    dcache.save_modified_data(msg) # may raise SystemExit
    dcache.save_cache()
    raise SystemExit(0)


def parse_args(args=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('-v', '--verbose', action='store_true')
    ap.add_argument('--use-mason', action='store_true')
    ap.add_argument('--use-local-submodules', action='store_true')
    ap.add_argument('--deadline', metavar='TIMESTAMP', default=None, type=int)
    ap.add_argument('--data-dir', metavar='PATH', default='./data')
    ap.add_argument('--max-samples', metavar='N', default=15, type=int, choices=range(1, 30))
    ap.add_argument('--since', metavar='DATE', default='2015-07-04',
                    help='date in format accepted by git log')
    ap.add_argument('--source-repository', metavar='URL', required=True)
    ap.add_argument('branches', metavar='BRANCH', default=['master'], nargs='*')
    return ap.parse_args(args, ArgumentNamespace())


def main(args=None):
    try:
        global ARGS
        ARGS = parse_args(args)
        _main()
    except SystemExit as ex:
        return ex.code


if __name__ == "__main__":
    ARGS = parse_args()
    _main()
