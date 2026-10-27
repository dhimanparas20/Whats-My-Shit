import os
import redis
from IPython.core.getipython import get_ipython
from IPython.core.magic import register_line_magic

ip = get_ipython()
r = redis.from_url(os.getenv("REDIS_URL"), decode_responses=True)

# Enable autoreload
if ip is not None:
    try:
        ip.run_line_magic("alias", "cls clear")
    except Exception:
        pass
    ip.run_line_magic("load_ext", "autoreload")
    ip.run_line_magic("load_ext", "rich")
    ip.run_line_magic("autoreload", "2")