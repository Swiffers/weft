CREATE TABLE IF NOT EXISTS role_loop_due (
            -- The role (`CoreRole::as_str`) and one of its loops
            -- (`DrainLoop::name`).
            role TEXT NOT NULL,
            loop_name TEXT NOT NULL,
            -- When the loop next wants a look, in unix milliseconds on the
            -- database's clock, so instances whose own clocks disagree
            -- still agree on it. Empty while it has nothing to look at
            -- until a write wakes it.
            due_ms BIGINT,
            -- When this row was last written, on the same clock: how a
            -- pass tells whether a sibling booked a look since it read.
            written_ms BIGINT NOT NULL,
            -- How many bookings this row has had: the generation of the
            -- wake set for `due_ms`, so a wake set again never reuses the
            -- name of one delivered or cancelled.
            seq BIGINT NOT NULL DEFAULT 0,
            -- How many times a tick the wake of booking `seq` arrived
            -- early at set it again (`BookedLook::set_again`): the wake
            -- of this booking still waiting is the one under that count,
            -- which a booking that moves cancels.
            set_again INT NOT NULL DEFAULT 0,
            -- How many of the loop's drains in a row failed: which of its
            -- retries comes next (`RETRY_PAUSES`).
            failures INT NOT NULL DEFAULT 0,
            -- Whether a failure of the loop is written down for `weft
            -- status` (`unanswered_call`): its next drain that goes
            -- through clears it.
            failure_written BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY (role, loop_name)
        );
