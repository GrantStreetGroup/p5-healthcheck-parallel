package HealthCheck::Parallel;

use v5.10;
use strict;
use warnings;

use parent 'HealthCheck';

use Carp;
use Parallel::ForkManager;

# ABSTRACT: A HealthCheck that uses parallelization for running checks
# VERSION

sub new {
    my ( $class, %params ) = @_;

    $params{max_procs} //= 4;
    $params{timeout}   //= 120;

    my $self = $class->SUPER::new( %params );

    $self->_validate_max_procs( $params{max_procs} );
    $self->_validate_child_init( $params{child_init} );
    $self->_validate_timeout( $params{timeout} );

    return $self;
}

sub _run_checks {
    my ( $self, $checks, $params ) = @_;

    $self->_validate_max_procs( $params->{max_procs} )
        if exists $params->{max_procs};

    $self->_validate_child_init( $params->{child_init} )
        if exists $params->{child_init};

    $self->_validate_timeout( $params->{timeout} )
        if exists $params->{timeout};

    my $max_procs  = $params->{max_procs}  // $self->{max_procs};
    my $child_init = $params->{child_init} // $self->{child_init};
    my $tempdir    = $params->{tempdir}    // $self->{tempdir};
    my $timeout    = $params->{timeout}    // $self->{timeout};

    my @results;
    my $forker;
    my $start_time;
    my $timed_out = 0;

    # Helper to kill all running child processes.
    my $kill_all_children = sub {
        my @running = $forker->running_procs;
        kill 'TERM', @running;
    };

    if ( $max_procs > 1 ) {
        $forker = Parallel::ForkManager->new(
            $max_procs,
            $tempdir ? $tempdir : (),
        );

        $forker->run_on_finish(sub {
            my ( $pid, $exit_code, $ident, $exit_sig, $core_dump, $ret ) = @_;

            # Child process had some error.
            if ( $exit_code != 0 ) {
                $results[ $ident ] = {
                    status => 'CRITICAL',
                    info   => "Child process exited with code $exit_code.",
                };
            }
            else {
                # Keep results in the same order that they were provided.
                $results[ $ident ] = $ret->[0];
            }
        });

        # Set up on_wait callback to check timeout during dispatch.
        # This is called periodically when start() is in its wait loop.
        $start_time = time;
        $forker->run_on_wait(sub {
            my $elapsed = time - $start_time;

            # Check if we've exceeded timeout.
            if ( $elapsed > $timeout ) {
                $timed_out = 1;
                # Kill all children and make start() exit its wait loop.
                $kill_all_children->();
            }
        }, 1);  # Check every 1 second
    }

    my $i = 0;
    for my $check ( @$checks ) {
        # Stop dispatching if timeout occurred.
        last if $timed_out;

        if ( $forker ) {
            $forker->start( $i++ ) and next;
            $child_init->() if $child_init;
        }

        my @r = $self->_run_check( $check, $params );

        $forker->finish( 0, \@r ) if $forker;

        # Non-forked process.
        push @results, @r;
    }

    if ( $forker ) {
        # If we already timed out during dispatch, skip polling since
        # children were already killed.
        if ( !$timed_out ) {
            # Poll for child completion with timeout checking.
            while ( $forker->running_procs ) {
                # Collect any finished children (non-blocking).
                $forker->reap_finished_children;

                # Check if we've exceeded timeout during polling.
                if ( time - $start_time > $timeout ) {
                    $timed_out = 1;
                    # Kill all still-running children.
                    $kill_all_children->();
                    last;
                }

                # Sleep before next check.
                sleep $forker->waitpid_blocking_sleep;
            }
        }

        # Final reap to collect any remaining children.
        $forker->reap_finished_children;

        # Die with timeout error if timeout occurred.
        die "Global timeout of ${timeout} seconds exceeded.\n" if $timed_out;
    }

    return @results;
}

sub _validate_max_procs {
    my ( $self, $max_procs ) = @_;

    croak "max_procs must be a zero or positive integer!"
        unless $max_procs =~ /^\d+$/ && $max_procs >= 0;
}

sub _validate_child_init {
    my ( $self, $child_init ) = @_;

    croak "child_init must be a code reference!"
        if defined $child_init && ref( $child_init ) ne 'CODE';
}

sub _validate_timeout {
    my ( $self, $timeout ) = @_;

    croak "timeout must be a positive integer!"
        unless $timeout =~ /^\d+$/ && $timeout > 0;
}

1;
__END__

=head1 SYNOPSIS

    use HealthCheck::Parallel;

    my $hc = HealthCheck::Parallel->new(
        max_procs  => 4,      # default
        timeout    => 120,    # default, global timeout in seconds
        tempdir    => '/tmp', # override Parallel::ForkManager default
        child_init => sub { warn "Will run at start of child process check" },
        checks     => [
            sub { sleep 5; return { id => 'slow1', status => 'OK' } },
            sub { sleep 5; return { id => 'slow2', status => 'OK' } },
        ],
    );

    # Takes 5 seconds to run both checks instead of 10.
    my $res = $hc->check;

    # These checks will not use parallelization.
    $res = $hc->check( max_procs => 0 );

    # Neither will these.
    $res = $hc->check( max_procs => 1 );

    # Override timeout for specific check.
    $res = $hc->check( timeout => 60 );

=head1 DESCRIPTION

This library inherits L<HealthCheck> so that the provided checks are run in
parallel.

=head1 METHODS

=head2 new

Overrides the L<HealthCheck/new> constructor to additionally allow
L</max_procs> and L</timeout> arguments for controlling parallelization
and global timeout behavior.

=head1 ATTRIBUTES

=head2 max_procs

A positive integer specifying the maximum number of processes that should be run
in parallel when executing the checks.
No parallelization will be used unless given a value that is greater than 1.
Defaults to 4.

=head2 child_init

An optional coderef which will be run when the child process of a check is
created.
A possible important use case is making sure child processes don't try to make
use of STDOUT if these checks are running under FastCGI envrionment:

    my $hc = HealthCheck::Parallel->new(
        child_init => sub {
            untie *STDOUT;
            { no warnings; *FCGI::DESTROY = sub {}; }
        },
    );

=head2 tempdir

Sets the C<tempdir> value to use in L<Parallel::ForkManager> for IPC.

=head2 timeout

A positive integer specifying the maximum number of seconds to wait for all
parallelized checks to complete.
If the timeout is exceeded, all running child processes will be terminated
and a CRITICAL status will be returned with a global timeout error.
Defaults to 120 seconds.

B<Note:> The timeout only applies when parallelization is enabled
(C<max_procs E<gt> 1>). When C<max_procs> is 0 or 1, checks run in the parent
process and the timeout is not used.

The timeout is implemented using a non-blocking polling loop instead of using
any signal-based timeouts to potentially avoiding conflicting with others.

=head1 DEPENDENCIES

=over 4

=item *

Perl 5.10 or higher.

=item *

L<HealthCheck>

=item *

L<Parallel::ForkManager>

=back

=head1 SEE ALSO

=over 4

=item *

L<HealthCheck::Diagnostic>

=item *

The GSG
L<Health Check Standard|https://grantstreetgroup.github.io/HealthCheck.html>.

=back
