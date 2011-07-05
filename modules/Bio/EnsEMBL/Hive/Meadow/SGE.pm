package Bio::EnsEMBL::Hive::Meadow::SGE;

use strict;

use base 'Bio::EnsEMBL::Hive::Meadow';

sub get_current_worker_process_id {
    my ($self) = @_;

#    my $sge_task_id = $ENV{'SGE_TASK_ID'};

#    if(defined($jobid) and defined($sge_task_id)) {
#        if($sge_task_id>0) {
#	    return "$jobid\[$sge_task_id\]";
#        } else {
#	    return $jobid;
#        }
#    }

    if( defined($ENV{'JOB_ID'}) ) {
	return $ENV{'JOB_ID'};
    } else {
        die "Could not establish the process_id";
    }
}

# Need to be checked
#sub count_running_workers {
#    my ($self) = @_;
#    
#    my $cmd = "qstat ";
#    my $jnp = $self->job_name_prefix();
#    $cmd .= " | grep '$jnp'";
#
#    $cmd .= qq{ | sed 's/  */\t/g' | cut -f5 | grep -c -v [qw] };   #"| grep -c RUN"[dEhrRsStT] ; The status of the job. The equal state for bjobs RUN is all but w or qw
#
#    my $run_count = qx/$cmd/;
#    chomp($run_count);
#
#    return $run_count;
#}

sub count_pending_workers {
    my ($self) = @_;

    my $cmd = "qstat ";
    my $jnp = $self->job_name_prefix();
    $cmd .= " | grep '$jnp'";

    $cmd .= qq{ | sed 's/  */\t/g' | cut -f5 | grep -c -v [dEhrRsStT] };   #"| grep -c PEND"[qwsStT] ; The status of the job. The equal state for bjobs PEND is w or qw

    my $pend_count = qx/$cmd/;
    chomp($pend_count);

    return $pend_count;
}

sub status_of_all_our_workers { # returns a hashref
    my ($self) = @_;

    my $cmd = qq{qstat 2>&1 | grep -v 'No unfinished job found' | grep -v -i job-ID | grep -v "^-" }; #grep -v DONE | grep -v EXIT

    my $jnp = $self->job_name_prefix();
    $cmd .= " | grep '$jnp'";
    
    my %status_hash = ();
    foreach my $line (`$cmd`) {
        my ($job_pid, $prior, $job_name, $user, $status, $sdate, $stime, $queue, $slots, $ja_task_ID) = split(/\s+/, $line);

        my $worker_pid = $job_pid;
        if($job_name=~/(\[\d+\])/) {
            $worker_pid .= $1;
        }
            
        $status_hash{$worker_pid} = $status;
    }
    return \%status_hash;
}

sub check_worker_is_alive_and_mine {
    my ($self, $worker) = @_;
    
    my $wpid = $worker->process_id();
    my $this_user = $ENV{'USER'};
    # Check that -u works as expected. Maybe use -U. Or forget about these options all together.
    my $cmd = 'qstat -u $this_user | grep "^$wpid"  2>&1 | grep -v "not found" | grep -v JOBID |  cut -f5 | grep -v [Ed]';

    my $is_alive_and_mine = qx/$cmd/;
    return $is_alive_and_mine;
}

sub kill_worker {
    my ($self, $worker) = @_;

    if($self->check_worker_is_alive_and_mine($worker)) {
        my $cmd = 'qdel '.$worker->process_id();
        system($cmd);
    } else {
        warn 'Cannot kill worker '.$worker->process_id().' because it is not running';
    }
}

sub generate_job_name {
    my ($self, $worker_count, $iteration, $rc_id) = @_;
    $rc_id ||= 0;

    return $self->job_name_prefix()
        ."${rc_id}_${iteration}";
}

sub submit_workers {
    my ($self, $iteration, $worker_cmd, $worker_count, $rc_id, $rc_parameters) = @_;

    my $job_name    = $self->generate_job_name($worker_count, $iteration, $rc_id);
    my $meadow_options = $self->meadow_options();

# This is for array job. Untested
#    my $cmd;
#    if ($worker_count > 1) {
#	$cmd = "qsub -V -N \"${job_name}\" -t 1-$worker_count $meadow_options -b y $worker_cmd";
#    } else {
#	$cmd = "qsub -V -N \"${job_name}\" $meadow_options -b y $worker_cmd";
#    }
#
#    print "SUBMITTING_CMD:\t\t$cmd\n";
#    system($cmd) && die "Could not submit job(s): $!, $?";  # let's abort the beekeeper and let the user check the syntax

    for (my $worker_number=1; $worker_number <= $worker_count; $worker_number++){
 	my $cmd = "qsub -V -o /dev/null -e /dev/null -N \"${job_name}-${worker_number}\" $meadow_options -b y $worker_cmd";

    	print "SUBMITTING_CMD:\t\t$cmd\n";
	system($cmd) && die "Could not submit job(s): $!, $?";  # let's abort the beekeeper and let the user check the syntax
    }
}

1;
