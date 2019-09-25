from typing import List, Union

import operator as op
from dask.distributed import (
    get_client, wait, Client, Future
)


def create_client(**kwargs):
    return Client(**kwargs)


class FutureWrapper(Future):

    def __init__(self,  key, client=None, inform=True, state=None):
        Future.__init__(self,  key, client, inform, state)

    @classmethod
    def from_future(cls, future: Future):
        return FutureWrapper(future.key, future.client, future._state)

    def __hash__(self):
        return Future.__hash__(self)

    def __getitem__(self, item):
        client = get_client()
        new_fut = client.submit(op.getitem, self, item)
        return FutureWrapper.from_future(new_fut)

    def __eq__(self, other):
        client = get_client()
        new_fut = client.submit(op.eq, self, other)
        return FutureWrapper.from_future(new_fut)

    def __and__(self, other):
        client = get_client()
        new_fut = client.submit(op.and_, self, other)
        return FutureWrapper.from_future(new_fut)

    def __or__(self, other):
        client = get_client()
        new_fut = client.submit(op.or_, self, other)
        return FutureWrapper.from_future(new_fut)

    def __xor__(self, other):
        client = get_client()
        new_fut = client.submit(op.xor, self, other)
        return FutureWrapper.from_future(new_fut)

    def __add__(self, other):
        client = get_client()
        new_fut = client.submit(op.add, self, other)
        return FutureWrapper.from_future(new_fut)

    def __mul__(self, other):
        client = get_client()
        new_fut = client.submit(op.mul, self, other)
        return FutureWrapper.from_future(new_fut)

    def __sub__(self, other):
        client = get_client()
        new_fut = client.submit(op.sub, self, other)
        return FutureWrapper.from_future(new_fut)

    def __mod__(self, other):
        client = get_client()
        new_fut = client.submit(op.mod, self, other)
        return FutureWrapper.from_future(new_fut)

###################
# Types
###################


FS = Union[List[FutureWrapper], FutureWrapper]


def while_loop(cond, body, loop_vars):
    """
    Parameters
    ----------
    cond: Callable
        a callable returning a boolean, arguments are all loop_vars,
        should be serializable
    body: Callable
        a callable returning anything, arguments are all loop_vars, should
        be serializable
    loop_vars:
        all loop variables needed, can be Future or any other
        serializable object.

    Returns
    -------
    future: FutureWrapper
        a FutureWrapper object representing a updated loop_vars
    """
    client = get_client()

    def _while_task(c, b, lv=None):
        if not lv:
            lv = []
        updated_lv = lv
        while c(updated_lv):
            updated_lv = b(updated_lv)
            if updated_lv is None:
                updated_lv = []

        return updated_lv
    future = client.submit(_while_task, cond, body, loop_vars)
    return FutureWrapper.from_future(future)


def for_loop(iterable, body, loop_vars=None, enumerate=False):
    """

    Parameters
    ----------
    iterable:
        Iterable FutureWrapper/object to iterate over
    body:
        a callable returning updated loop_vars, arguments are items in iterable
        and all loop_vars. body(x, loop_vars), where x is an instances of
        elements in the iterator and loop_vars are all loop variables.
        method should be serializable.
    loop_vars:
        all loop variables needed, can be Future or any other
        serializable object.
    enumerate:
        True to use for x in enumerate(iterable) instead of x in iterable

    Returns
    -------
        a FutureWrapper object representing a updated loop_vars
    """
    client = get_client()

    def _for_task(iterable, b, lv=None):
        if not lv:
            lv = []
        updated_loop_vars = lv
        for x in iterable:
            updated_loop_vars = b(x, updated_loop_vars)
            if updated_loop_vars is None:
                updated_loop_vars = []

        return updated_loop_vars

    if enumerate:
        iterable = client.submit(enumerate, iterable)
    future = client.submit(_for_task, iterable, body, loop_vars)
    return FutureWrapper.from_future(future)


# noinspection PyIncorrectDocstring
def get_results(fs: FS, errors='raise', direct=None, asynchronous=None):
    """
    Gather futures from distributed memory

    Accepts a future, nested container of futures, iterator, or queue.
    The return type will match the input type.

    Parameters
    ----------
    fs: Collection of futures
        This can be a possibly nested collection of Future objects.
        Collections can be lists, sets, or dictionaries
    errors: string
        Either 'raise' or 'skip' if we should raise if a future has erred
        or skip its inclusion in the output collection
    direct: boolean
        Whether or not to connect directly to the workers, or to ask
        the scheduler to serve as intermediary.  This can also be set when
        creating the Client.

    Returns
    -------
    results: a collection of the same type as the input, but now with
    gathered results rather than futures
    """
    client = get_client()
    if isinstance(fs, FutureWrapper):
        fs = [fs]
    return client.gather(fs, errors, direct, asynchronous)


def synchronize(fs: FS, timeout=None,
                return_when="ALL_COMPLETED"):
    """
    Wait until all/any futures are finished

    Parameters
    ----------
    fs: list of futures
    timeout: number, optional
        Time in seconds after which to raise a ``dask.distributed.TimeoutError``
    return_when:
        ALL_COMPLETED
        FIRST_COMPLETED
    -------
    Named tuple of completed, not completed
    """
    if isinstance(fs, FutureWrapper):
        fs = [fs]
    if return_when is None:
        return_when = 'ALL_COMPLETED'
    return wait(fs, timeout, return_when)
