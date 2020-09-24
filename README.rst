akun-tools
==========

akun-tools is a set of tools for interacting with Fiction.live, previously known
as anonkun. Currently it includes a scraper that generates HTML ebooks of quests.

akun-tools is a command-line utility that requires Python 3 and pip. To install
it, run:

.. code:: bash

  $ pip install akun-tools

Once installed, you run it with ``akun_scraper``. Example usage:

.. code:: bash

  $ akun_scraper getinfo https://fiction.live/stories/Abyssal-Admiral-Quest-/QGSpwAtjf9NMtvsTA/home
  $ akun_scraper download abyssal_admiral_quest.toml
