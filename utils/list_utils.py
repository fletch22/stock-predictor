import math
from ipython_genutils.py3compat import xrange

from config import logger_factory

logger = logger_factory.create_logger(__name__)

def chunk_list(the_list, num_chunks, bunch_up_if_small=False):
  if len(the_list) < num_chunks and bunch_up_if_small:
    num_chunks = 1
  sublist_size = math.ceil(len(the_list) / num_chunks)
  logger.info(f"Will divide symbols into sublists of size: {sublist_size}")
  return [the_list[x:x + sublist_size] for x in xrange(0, len(the_list), sublist_size)]