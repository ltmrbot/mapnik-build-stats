import os.path
import yamlutil
from random import randint
from time import time


def days_ago(days, now=time()):
    return now - days * 24 * 3600


REFRESH_THRESHOLD = days_ago(randint(14, 28))


class DataCache(object):

    def __init__(self, cache_root, *, data_repo, targets):
        self.cache_root = cache_root
        self.data_repo = data_repo
        self.targets = frozenset(targets)
        self.sdata_cache = {}
        self.sdata_modified = set()
        try:
            cache_file = self._cache_file_path()
            self._persistent = yamlutil.load_file(cache_file)
            print(F'\nloaded {cache_file}')
        except Exception:
            self._persistent = {}

    def _cache_file_path(self):
        revs = self.data_repo.git_rev_list('-1', 'HEAD', '--')
        sha1 = max(line.strip() for line in revs)
        return os.path.join(self.cache_root, F'data_at_{sha1}.yml')

    def _persist_dict(self, *keys):
        d = self._persistent
        for k in keys:
            try:
                d = d[k]
            except KeyError:
                d[k] = d = {}
        return d

    def _prune_source_data(self, sdata):
        """
        Remove very old build records.

        """
        modified = False
        for builds in sdata.values():
            recent = [r for r in builds
                        if r.get('timestamp', 0) >= days_ago(360)]
            if len(builds) > len(recent):
                builds[:] = recent
                modified = True
        return modified

    def _source_data_file(self, src_path, arg_hash):
        return os.path.join(self.data_repo.dir, 'sources',
                            arg_hash[:2], arg_hash, F'{src_path}.yml')

    def _update_commit_metadata(self, commit):
        meta = commit.metadata()
        self._persist_dict('commits', commit.sha1)['metadata'] = meta
        return meta

    def _update_compile_timestamps(self, sdata, src_path, arg_hash):
        d = self._persist_dict('compiles', arg_hash, src_path)
        for cpp_hash, ts in d.items():
            if cpp_hash not in sdata:
                # clear list but leave it in the cache
                del ts[:]
        for cpp_hash, rs in sdata.items():
            ts = d.setdefault(cpp_hash, [])
            ts[:] = (r['timestamp'] for r in rs)

    def get_commit_metadata(self, commit):
        try:
            return self._persistent['commits'][commit.sha1]['metadata']
        except:
            return None

    def get_commit_sources(self, commit):
        return commit.sources()

    def last_commit_refresh(self, commit):
        assert len(self.targets)
        meta = self.require_commit_metadata(commit)
        saved_targets = meta.get('targets')
        if saved_targets is not None:
            if self.targets.issubset(saved_targets):
                return meta.get('last_refresh', 0)
        return 0

    def require_commit_metadata(self, commit):
        try:
            return self._persistent['commits'][commit.sha1]['metadata']
        except:
            pass
        # FIXME verbose
        print(F'\nreading {commit}')
        ok = commit.load_metadata(data_root=self.data_repo.dir,
                                  Loader=yamlutil.InterningLoader)
        res = commit.metadata()
        self._persist_dict('commits', commit.sha1)['metadata'] = res
        return res

    def require_commit_sources(self, commit, code_repo):
        res = commit.sources()
        if res is None:
            last_refresh = self.last_commit_refresh(commit)
            if last_refresh >= REFRESH_THRESHOLD:
                if commit.load_sources(data_root=self.data_repo.dir,
                                       Loader=yamlutil.InterningLoader):
                    res = commit.sources()
                else:
                    # FIXME verbose
                    print('failed to load sources for', commit)
        if res is None:
            commit.update_sources(code_repo, self.targets)
            self._update_commit_metadata(commit)
            res = commit.sources()
        return res

    def require_compile_timestamps(self, src_path, arg_hash, cpp_hash):
        try:
            d = self._persistent['compiles'][arg_hash][src_path]
        except KeyError:
            sdata = self.require_source_data(src_path, arg_hash)
            d = self._persistent['compiles'][arg_hash][src_path]
        return d.setdefault(cpp_hash, [])

    def require_source_data(self, *skey):
        sdata = self.sdata_cache.get(skey)
        if sdata is None:
            try:
                s_file = self._source_data_file(*skey)
                sdata = yamlutil.load_file(s_file)
                if self._prune_source_data(sdata):
                    self.sdata_modified.add(skey)
            except Exception:
                sdata = {}
            self.sdata_cache[skey] = sdata
        self._update_compile_timestamps(sdata, *skey)
        return sdata

    def save_cache(self):
        cache_file = self._cache_file_path()
        print(F'\nsaving {cache_file}')
        yamlutil.save_file(cache_file, self._persistent)

    def save_commit_data(self, commit):
        commit.save_data(data_root=self.data_repo.dir)

    def save_source_data(self, *skey):
        sdata = self.sdata_cache.get(skey)
        if sdata is not None:
            # Save data file first, ...
            s_file = self._source_data_file(*skey)
            yamlutil.save_file(s_file, sdata)
            self.sdata_modified.discard(skey)
            # ... then update timestamp cache.
            self._update_compile_timestamps(sdata, *skey)

    def save_modified_data(self, message):
        for skey in list(self.sdata_modified):
            self.save_source_data(*skey)
        self.data_repo.git('add', '--all')
        self.data_repo.git('commit', '-m', message)
        # if there's nothing to commit, git will report that fact,
        # and this will raise SystemExit

#endclass
