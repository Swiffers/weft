# migrations

One directory per schema group, holding that group's history.

`origin.sql` is the group's `CREATE TABLE` frozen on the day it shipped:
`./setup.sh --migration` writes it for a group that has none, in the same run
that plans the migration. Every
other file is one part of a change since, named so the files sort in the order
they were written: `<id>.sql` holds what the change adds, and
`<id>.contract.sql` what it drops and tightens. A booting weft runs every
pending `.sql` file, then carries the JSON documents forward, then runs every
pending `.contract.sql` file.

Nothing in here is ever edited after it has run. If a file is wrong, write
another one.

A database that already exists runs the files it has not run before, oldest
first within each part. A database being created for the first time skips them all and is built
from the canonical `CREATE TABLE` in the Rust source, which is where you edit
the schema.

`drafts/` holds migrations that exist only on the machine that wrote them:
gitignored, applied to that one database while a shape is still being decided,
and collapsed into a single released file by
`./setup.sh --migration <name> --release`. A draft pass is optional: a release
on a database that never saw one runs the released file on it right there.

Nothing lists these files. The crate's build script walks this directory, so a
file being here is the whole of it being a migration. It does so only with the
crate's `migrate` feature, which the roles that boot a database turn on. A
worker's build is given the crate without this folder, so adding a migration
does not rebuild any program or ask anyone to resync. How to write one, and the
test that keeps the two sides honest, is in
[CONTRIBUTING](../../../CONTRIBUTING.md#working-on-the-database).
