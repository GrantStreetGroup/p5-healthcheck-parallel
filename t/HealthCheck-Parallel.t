use Test2::V0 -target => 'HealthCheck::Parallel',
    qw< ok is like diag note done_testing >;

diag(qq($CLASS Perl $], $^X));

ok CLASS, "Loaded $CLASS";

{
    note "Successful basic results";

    my $hc = HealthCheck::Parallel->new(
        checks => [
            sub { return { id => 'id1', status => 'OK' } },
            sub { return { id => 'id2', status => 'OK' } },
        ],
    );

    is $hc->check, {
        status  => 'OK',
        results => [
            { id => 'id1', status => 'OK' },
            { id => 'id2', status => 'OK' },
        ],
    }, 'Got expected results with parallelization.';

    is $hc->check( max_procs => 0 ), {
        status  => 'OK',
        results => [
            { id => 'id1', status => 'OK' },
            { id => 'id2', status => 'OK' },
        ],
    }, 'Got expected results with no parallelization.';
}

{
    note "child_init process exit error";

    my $hc = HealthCheck::Parallel->new(
        checks     => [ sub { return { id => 'wontrun', status => 'OK' } } ],
        child_init => sub { exit 222 },
    );

    is $hc->check, {
        status => 'CRITICAL',
        info   => 'Child process exited with code 222.',
    }, 'Got expected results with child_init process exit error.';
}

{
    note "check process exit error";

    my $hc = HealthCheck::Parallel->new(
        checks => [
            sub { exit 111 },
            sub { return { id => 'ok', status => 'OK' } },
        ],
    );

    is $hc->check, {
        status  => 'CRITICAL',
        results => [
            {
                info   => 'Child process exited with code 111.',
                status => 'CRITICAL',
            },
            {
                id     => 'ok',
                status => 'OK',
            },
        ],
    }, 'Got expected results with child process exit error.'
}

{
    note "Invalid max_procs";

    my $err = 'max_procs must be a zero or positive integer!';

    local $@;
    eval { HealthCheck::Parallel->new( max_procs => -1 ) };
    like $@, qr/^\Q$err\E/,
        'Threw expected exception for invalid max_procs constructor.';

    my $hc = HealthCheck::Parallel->new(
        checks => [ sub { return { id => 'wontrun', status => 'OK' } } ],
    );
    my $r = $hc->check( max_procs => -1 );
    like $r, {
        status => 'CRITICAL',
        info   => qr/^\Q$err\E/,
    }, 'Got expected result for invalid max_procs parameter.';
}

{
    note "Invalid child_init";

    my $err = 'child_init must be a code reference!';

    local $@;
    eval { HealthCheck::Parallel->new( child_init => ['nope'] ) };
    like $@, qr/^\Q$err\E/,
        'Threw expected exception for invalid child_init constructor.';

    my $hc = HealthCheck::Parallel->new(
        checks => [ sub { return { id => 'wontrun', status => 'OK' } } ],
    );
    my $r = $hc->check( child_init => { wont => 'work' } );
    like $r, {
        status => 'CRITICAL',
        info   => qr/^\Q$err\E/,
    }, 'Got expected result for invalid child_init parameter.';
}

{
    note "Invalid timeout";

    my $err = 'timeout must be a positive integer!';

    local $@;
    eval { HealthCheck::Parallel->new( timeout => 0 ) };
    like $@, qr/^\Q$err\E/,
        'Threw expected exception for invalid timeout (0) constructor.';

    local $@;
    eval { HealthCheck::Parallel->new( timeout => -1 ) };
    like $@, qr/^\Q$err\E/,
        'Threw expected exception for invalid timeout (negative) constructor.';

    local $@;
    eval { HealthCheck::Parallel->new( timeout => 'notanumber' ) };
    like $@, qr/^\Q$err\E/,
        'Threw expected exception for invalid timeout (string) constructor.';

    my $hc = HealthCheck::Parallel->new(
        checks => [ sub { return { id => 'wontrun', status => 'OK' } } ],
    );
    my $r = $hc->check( timeout => 0 );
    like $r, {
        status => 'CRITICAL',
        info   => qr/^\Q$err\E/,
    }, 'Got expected result for invalid timeout parameter.';
}

{
    note "Global timeout during dispatch phase (on_wait callback)";

    my $hc = HealthCheck::Parallel->new(
        max_procs => 2,      # Force waiting during dispatch
        timeout   => 3,
        checks    => [
            sub { sleep 10; return { id => 'slow1', status => 'OK' } },
            sub { sleep 10; return { id => 'slow2', status => 'OK' } },
            sub { sleep 10; return { id => 'slow3', status => 'OK' } },
            sub { sleep 10; return { id => 'slow4', status => 'OK' } },
        ],
    );

    my $r = $hc->check;

    is $r, {
        status  => 'CRITICAL',
        results => [
            { status => 'CRITICAL', info => 'Check killed due to global timeout of 3 seconds.' },
            { status => 'CRITICAL', info => 'Check killed due to global timeout of 3 seconds.' },
            # One more check gets forked after timeout during check dispatching.
            { status => 'CRITICAL', info => 'Check killed due to global timeout of 3 seconds.' },
            { status => 'CRITICAL', info => 'Check not started due to global timeout of 3 seconds.' },
        ],
    }, 'Got expected timeout result during dispatch phase.';
}

{
    note "Global timeout during polling phase (after all dispatched)";

    my $hc = HealthCheck::Parallel->new(
        timeout => 2,
        checks  => [
            sub { sleep 10; return { id => 'slow1', status => 'OK' } },
            sub { sleep 10; return { id => 'slow2', status => 'OK' } },
            sub { sleep 10; return { id => 'slow3', status => 'OK' } },
        ],
    );

    my $r = $hc->check;

    is $r, {
        status  => 'CRITICAL',
        results => [
            { status => 'CRITICAL', info => 'Check killed due to global timeout of 2 seconds.' },
            { status => 'CRITICAL', info => 'Check killed due to global timeout of 2 seconds.' },
            { status => 'CRITICAL', info => 'Check killed due to global timeout of 2 seconds.' },
        ],
    }, 'Got expected timeout result during polling phase.';
}

{
    note "Global timeout with mixed fast and slow checks";

    my $hc = HealthCheck::Parallel->new(
        timeout => 5,
        checks  => [
            sub { return { id => 'fast1', status => 'OK' } },
            sub { sleep 10; return { id => 'slow1', status => 'OK' } },
            sub { return { id => 'fast2', status => 'OK' } },
        ],
    );

    my $r = $hc->check;

    is $r, {
        status  => 'CRITICAL',
        results => [
            { id => 'fast1', status => 'OK' },
            { status => 'CRITICAL', info => 'Check killed due to global timeout of 5 seconds.' },
            { id => 'fast2', status => 'OK' },
        ],
    }, 'Got expected timeout result with mixed fast and slow checks.';
}

{
    note "Checks complete before timeout";

    my $hc = HealthCheck::Parallel->new(
        timeout => 10,
        checks  => [
            sub { sleep 1; return { id => 'quick1', status => 'OK' } },
            sub { sleep 1; return { id => 'quick2', status => 'OK' } },
        ],
    );

    my $r = $hc->check;

    is $r, {
        status  => 'OK',
        results => [
            { id => 'quick1', status => 'OK' },
            { id => 'quick2', status => 'OK' },
        ],
    }, 'Got expected results when checks complete before timeout.';
}

{
    note "Single check completes successfully";

    my $hc = HealthCheck::Parallel->new(
        checks => [
            sub { return { id => 'single', status => 'OK', extra => 'data' } },
        ],
    );

    my $r = $hc->check;

    # Single check should return unwrapped result, not { results => [...] }.
    is $r, {
        id     => 'single',
        status => 'OK',
        extra  => 'data',
    }, 'Single check returns unwrapped result.';
}

{
    note "Timeout parameter override";

    my $hc = HealthCheck::Parallel->new(
        checks  => [
            sub { sleep 10; return { id => 'slow', status => 'OK' } },
        ],
    );

    my $r = $hc->check( timeout => 2 );

    is $r, {
        status => 'CRITICAL',
        info   => 'Check killed due to global timeout of 2 seconds.',
    }, 'Got expected timeout result with parameter override.';
}

done_testing;
