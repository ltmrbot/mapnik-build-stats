import shlex
from subprocess import DEVNULL, PIPE, Popen
from time import gmtime, strftime, time

verbose = False


def _log_command(args, highlight, hicolor):
    if verbose or args[0] == 'git':
        quoted = list(map(shlex.quote, args))
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
    _log_command(args, highlight, 94)
    return Popen(args, stdin=DEVNULL, **kwds)


def popen2(*args, highlight=[0], **kwds):
    args = tuple(flatten(*args))
    _log_command(args, highlight, 96)
    kwds.setdefault('universal_newlines', True)
    return Popen(args, stdin=DEVNULL, stdout=PIPE, **kwds)


def days_ago(days, *, since=time()):
    return since - days * 86400


def strdatetime(ts):
    return strftime("%Y-%m-%d %H:%M:%S", gmtime(ts))


def strsecsince(ts, *, width=0, prec=0):
    diff = time() - ts
    return F"{diff:+{width}.{prec}f}s"


async def async_popen(program, *args, highlight=[0],
                      stdin=None, stdout=None, stderr=None,
                      stdout_filter=None,
                      loop=None, **kwds):
    """
    """
    import asyncio
    from asyncio.subprocess import Process

    if loop is None:
        loop = asyncio.get_event_loop()

    if stdout is None:
        hicolor = 94
    elif stdout == DEVNULL:
        hicolor = 34
    else:
        hicolor = 96

    argv = tuple(flatten(program, *args))
    _log_command(argv, highlight, hicolor)

    if stdout is None:
        if stdout_filter is not None:
            stdout = PIPE
    else:
        if stdout_filter is not None:
            raise ValueError("stdout and stdout_filter may not both be used")
        if stdout == PIPE:
            stdout_filter = asyncio.StreamReader(loop=loop)

    if stderr == PIPE:
        stderr_filter = asyncio.StreamReader(loop=loop)
    else:
        stderr_filter = None

    def protocol_factory():
        from asyncutil import SubprocessFilterProtocol
        return SubprocessFilterProtocol(stdout=stdout_filter,
                                        stderr=stderr_filter,
                                        loop=loop)

    transport, protocol = await loop.subprocess_exec(protocol_factory, *argv,
                                                     stdin=stdin, stdout=stdout,
                                                     stderr=stderr, **kwds)
    proc = Process(transport, protocol, loop)
    proc.args = transport.get_extra_info("subprocess").args
    return proc
#enddef


#endfile
