= moist =

moist is a Python database adaptor for MySQL, MariaDB, and
(eventually) Drizzle. It's a continuation of the development
fork of MySQLdb, i.e. pre-2.0.

moist is not yet ready for prime-time, and there are likely to
be some large structural changes in the near future. Despite
this, since it will be DB-API 2.0-compatible, it should still
mostly be a drop-in replacement for MySQLdb. Some of the types
returned may be different, and if you use custom type conversion,
this will change a lot, but you'll like it. Trust me.

moist will require at least Python-2.7 and also be compatible
with Python-3.x, but isn't just yet. It should pass all the
old unit tests from MySQLdb, but that doesn't mean strange things
won't happen if try to use it in code that currently uses
MySQLdb. Feel free to try it, and submit patches/pull requests.

