-- Written by hand (what a queued event waits on moved from instance_gap to
-- waits_for, which changes values in rows and no generated migration sees):
-- an event a worker queued for a missing instance value says so in
-- waits_for, the one column the install reads now. instance_gap stays, unread,
-- until a later release drops it.
UPDATE parked_fire
SET waits_for = 'instance: ' || (instance_gap #>> '{}')
WHERE instance_gap IS NOT NULL
  AND waits_for IS NULL;
