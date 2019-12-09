#! /usr/bin/env python3

import argparse
import asyncio
import os
import random
import shlex
import shutil
import tempfile
from random import randint
from time import time

import procutil
from datacache import DataCache
from gitutil import GitRepo
from procutil import check_exit, popen, days_ago, strdatetime


REFRESH_THRESHOLD = days_ago(randint(14, 28))
REFRESH_THRESHOLD = max(1575734000, REFRESH_THRESHOLD)


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

    def _config_status(self):
        return os.path.join(self.dir, 'config.commit_and_status')

    def checkout(self, commit):
        self.git('submodule', 'deinit', '--force', '--all')
        self.git('checkout', '--force', commit.sha1, '--')
        if commit.cdate < 1511519354: # 2017-11-24 11:29:14 +0100
            self.python = 'python2'
        else:
            self.python = 'python3'
            # 8cd2ae322 2017-11-22 14:56:20 +0100 "support both python 2 and 3."
            # b0f3f1aed 2017-11-24 11:29:14 +0100 "port Scons3 support from master"
            proc2to3 = popen("find -name scons -prune , -name build.py -exec 2to3-2.7 --write '{}' +",
                             shell=True, cwd=self.dir)
            proc2to3.wait() # ignore result

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
        proc = popen('bash', 'bootstrap.sh',
                     highlight=[0,1], cwd=self.dir, stderr=stderr)
        if proc.wait() == 0:
            self.bootstrap_ok = nok + 1
        print(F'bootstrap.sh returned {proc.returncode}')
        return proc.returncode

    def configure(self):
        if ARGS.use_mason:
            returncode = self.bootstrap()
            if returncode != 0:
                return returncode
        configure_script = F'''
source ./mapnik-settings.env || true
{shlex.quote(self.python)} scons/scons.py --implicit-deps-changed \\
    configure \
CC={shlex.quote(os.environ.get("CC", "cc"))} \
CXX={shlex.quote(os.environ.get("CXX", "c++"))}
'''
        with open(self._config_status(), 'w') as fw:
            proc = popen('bash', '-c', configure_script, 'configure.bash',
                         highlight=[0,2], cwd=self.dir)
            self.git('rev-parse', 'HEAD', stdout=fw)
            print(proc.wait(), file=fw)
        print(F'configure returned {proc.returncode}')
        return proc.returncode

    def checkout_and_configure(self, commit):
        try:
            with open(self._config_status(), 'r') as fr:
                cfg_sha1 = fr.readline().strip()
                cfg_status = int(fr.readline())
        except Exception as ex:
            print(ex)
            print(F"couldn't read {self._config_status()}")
        else:
            print(F"previously configured, status {cfg_status}, commit {cfg_sha1}")
            if cfg_sha1 == commit.sha1 == self.tip_sha1():
                print("still on that commit, skipping configure")
                return cfg_status
        self.clean()
        self.checkout(commit)
        return self.configure()

    async def scons(self, *args, **kwds):
        from procutil import async_popen
        return await async_popen(self.python, 'scons/scons.py', *args,
                                 highlight=[0,1], cwd=self.dir, **kwds)

    async def get_build_commands(self, *targets):
        from subprocess import CalledProcessError, DEVNULL, PIPE
        print('Generating build commands...', *targets)
        proc = await self.scons('--dry-run', '--no-cache', *targets,
                                stdin=DEVNULL, stdout=PIPE)
        async for bline in proc.stdout:
            yield bline.decode()
        if await proc.wait() != 0:
            raise CalledProcessError(proc.returncode, proc.args)

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
    if not compile_timestamps:
        return 0
    n = len(compile_timestamps)
    latest = compile_timestamps[-1]
    multiplier = 3600 * (1.5 * n - 0.5 / n)
    return latest + base_delay_hours * multiplier
#enddef


async def consider_commit(c, repo, dcache):
    try:
        configure_ok = dcache.was_configure_ok(c)
        if configure_ok is False:
            sources = None
        else:
            sources = dcache.require_commit_sources(c)
            if sources is None:
                config_status = await c.update_sources(repo, dcache.targets)
                dcache.update_commit_metadata(c)
                dcache.save_commit_data(c)
                if config_status == 0:
                    sources = dcache.get_commit_sources(c)
        # A check just for None would not be sufficient here, because
        # SCons can still fail to print any build commands due to errors
        # in build scripts that were not included during configure step.
        if not sources:
            vprint('skipping because configure failed at'
                   F' {strdatetime(dcache.last_commit_refresh(c))}')
            return False
    finally:
        dcache.save_commit_data(c)

    ARGS.check_deadline()
    comp_tss = []
    for src_path, s in sources.items():
        cpp_hash = s.get('preprocessed_hash')
        arg_hash = s.get('filtered_args_hash')
        if cpp_hash is None:
            continue
        ARGS.check_deadline()
        ts = dcache.require_compile_timestamps(src_path, arg_hash, cpp_hash)
        comp_tss.append(ts)

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
    print(F'\n{full_builds} full builds, last compiled between'
          F' {strdatetime(least_recent_last) if least_recent_last else "NEVER"}'
          F' and {strdatetime(most_recent_last) if most_recent_last else "NEVER"}')
    return True


async def process_commit(c, repo, dcache):

    last_refresh = dcache.last_commit_refresh(c)
    if last_refresh < REFRESH_THRESHOLD:
        config_status = await c.update_sources(repo, dcache.targets)
        dcache.update_commit_metadata(c)
        dcache.save_commit_data(c)
    else:
        config_status = repo.checkout_and_configure(c)
    if config_status != 0:
        return

    now = time()
    tuples = []
    sources = dcache.require_commit_sources(c)
    for src_path, s in sources.items():
        cpp_hash = s.get('preprocessed_hash')
        arg_hash = s.get('filtered_args_hash')
        if cpp_hash is None:
            continue
        ts = dcache.require_compile_timestamps(src_path, arg_hash, cpp_hash)
        thres = next_compile_threshold(11, ts)
        if now < thres:
            continue
        tuples.append((thres, src_path, arg_hash, cpp_hash))
    if not tuples:
        return
    tuples.sort() # order by next_compile_threshold

    async def time_compile_one(src_path, arg_hash, cpp_hash):
        args = shlex.split(c.sources()[src_path]['compiler_args'])
        args += ['-o', src_path + '.o', src_path]
        cr = await repo.timed_command(*args)
        sdata = dcache.require_source_data(src_path, arg_hash,
                                           prune_before=days_ago(360))
        crs = sdata.setdefault(cpp_hash, [])
        crs.append(cr)
        if len(crs) > ARGS.max_samples:
            crs.sort(key=lambda cr: cr['timestamp'])
            del crs[:-ARGS.max_samples]
        dcache.save_source_data(src_path, arg_hash)

    print(F'\nTiming compilation, {len(tuples)} sources eligible')
    try:
        num_done = 0
        for thres, *params in tuples:
            ARGS.check_deadline()
            await time_compile_one(*params)
            num_done += 1
            if num_done % 75 == 0:
                # ignore ARGS.verbose:
                # - in non-verbose mode, we need to split long lines of dots
                # - in verbose mode, an extra line won't hurt
                print(F'\ncompiled {num_done}/{len(tuples)} sources, {c}')
    finally:
        if num_done % 75 != 0:
            print(F'\ncompiled {num_done}/{len(tuples)} sources, {c}')


def vprint(*args, **kwds):
    if ARGS.verbose:
        print(*args, **kwds)


async def _main():
    procutil.verbose = ARGS.verbose

    tmp = os.path.join(tempfile.gettempdir(), 'build-stats')
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp, exist_ok=False)

    cachedir = os.path.join(tempfile.gettempdir(), 'build-stats-cache')
    os.makedirs(cachedir, exist_ok=True)

    start = time()
    repo = MapnikGitRepo(ARGS.source_repository, os.path.join(tmp, 'mapnik'))
    repo.fetch_branches(ARGS.since, *ARGS.branches)
    commits = repo.commits_since(ARGS.since, *ARGS.branches)

    print(F'\nFound {len(commits)} commits')
    if not commits:
        raise SystemExit(0)

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
            if await consider_commit(c, repo, dcache):
                await process_commit(c, repo, dcache)
    except DeadlineReached as ex:
        print(F'\nreached deadline {strdatetime(ex.deadline)}')

    msg = (' '.join(ARGS.branches) +
           F' N={ARGS.max_samples} since={ARGS.since}')
    try:
        env = os.environ
        msg = (F'{env["TRAVIS_EVENT_TYPE"]} job {env["TRAVIS_JOB_NUMBER"]} {msg}\n\n'
               F'{env["TRAVIS_JOB_WEB_URL"]}\n'
               F'{env["TRAVIS_BUILD_WEB_URL"]}')
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


if __name__ == "__main__":
    ARGS = parse_args()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(_main())
    finally:
        loop.close()
