CREATE TABLE IF NOT EXISTS image_build (
            -- The content-addressed ref the build pushes: one build per
            -- content, whichever project asked for it.
            image_ref TEXT PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            -- The build's name, minted before it starts.
            build_name TEXT NOT NULL,
            -- Its id on the image builder, which the builder may choose
            -- (Cloud Build does): NULL until the start recorded it, and
            -- nobody asks the builder about a build without one.
            builder_id TEXT,
            -- The compile lane a worker build holds (see
            -- weft_compiler::worker_image::COMPILE_LANE_ARG); NULL for an
            -- image that compiles nothing.
            lane INTEGER,
            -- SYNC: the statuses <-> weft_core::projects::BuildState
            status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
            reason TEXT,
            -- Read only by dispatchers of an older release (see
            -- OLDER_READERS_START_HOLD_SECS); dropped in a later release.
            held_until BIGINT NOT NULL,
            started_at BIGINT NOT NULL,
            finished_at BIGINT,
            -- Since when the builder has failed to answer about it, every
            -- look since; NULL while it answers.
            failing_since BIGINT,
            -- Where a person reads the build's log, when the builder
            -- keeps one at an address.
            log_url TEXT
        );
CREATE TABLE IF NOT EXISTS image_use (
            project_id UUID NOT NULL,
            image_ref TEXT NOT NULL,
            running_since BIGINT NOT NULL,
            PRIMARY KEY (project_id, image_ref)
        );
CREATE TABLE IF NOT EXISTS image_claim (
            image_ref TEXT NOT NULL,
            claim_id UUID NOT NULL,
            -- The project whose build request holds the claim: what its
            -- status lists while the build runs, whoever started it. Every
            -- claim names one (`claim_images`).
            project_id UUID,
            holder_until BIGINT NOT NULL,
            PRIMARY KEY (image_ref, claim_id)
        );
CREATE TABLE IF NOT EXISTS version_build (
            -- Also the claim_id of the image_claim rows sparing the
            -- version's images while it waits.
            id UUID PRIMARY KEY,
            -- Its place in the project's order of asks (project.build_asks).
            ask BIGINT NOT NULL,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            -- The project's name, as the request named it.
            project_name TEXT NOT NULL,
            -- The version's files, registered as the program's source.
            manifest JSONB NOT NULL,
            -- The BuiltProgram the request compiled (replacedInfraImages
            -- empty: the registration fills it).
            program JSONB NOT NULL,
            -- Every image the version runs.
            images TEXT[] NOT NULL,
            -- The ImageBuilds it waits on, by image.
            waits_on JSONB NOT NULL,
            -- SYNC: the states <-> weft_core::builds::VersionBuildStatus
            state TEXT NOT NULL CHECK (state IN ('waiting', 'registered', 'failed', 'cancelled', 'superseded')),
            reason TEXT,
            -- The BuiltProgram answered once registered.
            registered JSONB,
            asked_at BIGINT NOT NULL,
            ended_at BIGINT
        );
CREATE UNIQUE INDEX IF NOT EXISTS version_build_one_waiting ON version_build (project_id) WHERE state = 'waiting';
