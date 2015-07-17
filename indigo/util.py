import collections
import functools
import uuid

def default_id():
   return unicode(uuid.uuid4())

class memoized(object):
   '''Decorator. Caches a function's return value each time it is called.
   If called later with the same arguments, the cached value is returned
   (not reevaluated).
   '''
   def __init__(self, func):
      self.func = func
      self.cache = {}

   def __call__(self, *args):
      if not isinstance(args, collections.Hashable):
         return self.func(*args)
      if args in self.cache:
         return self.cache[args]
      else:
         value = self.func(*args)
         self.cache[args] = value
         return value

   def __repr__(self):
      '''Return the function's docstring.'''
      return self.func.__doc__

   def __get__(self, obj, objtype):
      '''Support instance methods.'''
      return functools.partial(self.__call__, obj)

class IterStreamer(object):
    """
    File-like streaming iterator.
    """
    def __init__(self, generator):
        self.generator = generator
        self.iterator = iter(generator)
        self.leftover = ''

    def __len__(self):
        return self.generator.__len__()

    def __iter__(self):
        return self.iterator

    def next(self):
        return self.iterator.next()

    def read(self, size):
        data = self.leftover
        count = len(self.leftover)
        try:
            while count < size:
                chunk = self.next()
                data += chunk
                count += len(chunk)
        except StopIteration, e:
            self.leftover = ''
            return data

        if count > size:
            self.leftover = data[size:]

        return data[:size]