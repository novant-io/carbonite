# Test Setup for Posgres

Install using Homebrew:

    $ brew install postgresql

Start/stop postgres daemon:

    $ brew services start postgresql
    $ brew services stop postgresql

Configure test database:

    $ psql postgres

    # CREATE ROLE carbonite_test WITH LOGIN PASSWORD 'carbonite_pass';
    # ALTER ROLE carbonite_test CREATEDB;

Exit and restart `psql` with new user to validate:

    $ psql postgres -U carbonite_test

    # \du