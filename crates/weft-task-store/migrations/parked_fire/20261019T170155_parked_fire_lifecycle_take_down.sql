-- Throws away what is in parked_fire.instance_gap. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE parked_fire DROP COLUMN instance_gap;
