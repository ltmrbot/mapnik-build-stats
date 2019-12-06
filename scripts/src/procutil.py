from shlex import quote
from subprocess import DEVNULL, PIPE, Popen
from time import gmtime, strftime

verbose = False


def _print_command(args, highlight, hicolor):
    if verbose:
        quoted = list(map(quote, args))
        for i in highlight:
            quoted[i] = F'\033[{hicolor}m{quoted[i]}\033[m'
        print('\n' + ' '.join(quoted), flush=True)
    else:
        print('.', end='', flush=True)


def check_exit(proc):
    ret = proc.poll()
    if ret is None:
        proc.communicate()
        ret = proc.wait()
    if ret:
        raise SystemExit(ret)


def flatten(*args):
    for item in args:
        if isinstance(item, (tuple, list)):
            yield from flatten(*item)
        else:
            yield str(item)


def popen(*args, highlight=[0], **kwds):
    args = tuple(flatten(*args))
    _print_command(args, highlight, 94)
    return Popen(args, stdin=DEVNULL, **kwds)


def popen2(*args, highlight=[0], **kwds):
    args = tuple(flatten(*args))
    _print_command(args, highlight, 96)
    kwds.setdefault('universal_newlines', True)
    return Popen(args, stdin=DEVNULL, stdout=PIPE, **kwds)


def strdatetime(ts):
    return strftime("%Y-%m-%d %H:%M:%S", gmtime(ts))


#endfile
