import { describe, expect, it } from 'vitest';
import { addressOf, groupOfCallPath, specScopedTo, type RunSpec } from './run-spec';

describe('run spec', () => {
  it('reads the group the user stands in off the include prefix', () => {
    expect(groupOfCallPath([])).toBeNull();
    expect(groupOfCallPath(['c'])).toBe('c');
    expect(groupOfCallPath(['c', 'C.inner'])).toBe('c.inner');
    expect(groupOfCallPath(['c', 'C.billing.inner'])).toBe('c.billing.inner');
    expect(addressOf([], 'plain')).toBe('plain');
    expect(addressOf(['c'], 'C.strip')).toBe('c.strip');
    expect(addressOf(['c', 'C.inner'], 'Inner.deep')).toBe('c.inner.deep');
  });

  it('spells a node in a group, in an opened include, and a whole box, as weft run --target reads them', () => {
    // A group's member carries the group in its id: the place is the id.
    expect(addressOf([], 'triage.parse')).toBe('triage.parse');
    // A whole group box, or a whole include box, is its own id.
    expect(addressOf([], 'triage')).toBe('triage');
    expect(addressOf([], 'c')).toBe('c');
    // Inside the file `c` includes, a group in that file keeps its
    // name under the site.
    expect(addressOf(['c'], 'C.grp.x')).toBe('c.grp.x');
    // An include inside a group of the program: the site's id carries
    // the group, and the body's prefix comes off what it holds.
    expect(addressOf(['triage.c'], 'C.door')).toBe('triage.c.door');
  });

  it('knows which specs are scoped to a group', () => {
    const specs: RunSpec[] = [
      { name: 'whole' },
      { name: 'b-inner', from: { 'triage.parse': {} } },
      { name: 'a-group', group: ['triage', {}] },
      { name: 'other', group: ['billing', {}] },
    ];
    expect(specScopedTo(specs[0], 'triage')).toBe(false);
    expect(specScopedTo(specs[1], 'triage')).toBe(true);
    expect(specScopedTo(specs[2], 'triage')).toBe(true);
    expect(specScopedTo(specs[3], 'triage')).toBe(false);
  });
});
