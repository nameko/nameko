Releasing a new version to PyPI
-------------------------------

#. Make sure any merged features have entries in the change log (``CHANGES``)

#. Decide how much to bump the version (typically point version for backwards compatible changes, and minor version for backwards incompatible changes).

#. Bump the version in ``setup.py`` and in ``CHANGES``.

#. Update the release date in ``CHANGES``.

#. Unless this is already part of a feature branch, you may want to do all this in a dedicated ``vx.y.z-rc`` branch.

#. Merge your branch branch

#. Create a new `"release" <https://github.com/nameko/nameko/releases>`_ on github, named vx.y.z to match your version.

#. Publish the release, which will trigger a push to pypi from travis
