use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

struct Job {
    jobid: i64,
    ancestors: HashSet<i64>, // jobs this job depends on
    children: HashSet<i64>,  // jobs that depend on this job
}

impl Job {
    pub fn new(jobid: i64) -> Self {
        Self {
            jobid: jobid,
            ancestors: HashSet::new(),
            children: HashSet::new(),
        }
    }
}

#[derive(Debug,PartialEq)]
enum StateError {
    InvalidJobID,
    MissingDescendent,
    InvalidEvent,
}

struct State {
    jobs: HashMap<i64, Job>,
    /// Out symbol lookup table used when a new job with an `in` dependency is
    /// ingested.  We can look for that dependency in this table and find all
    /// relevant jobs to add as ancestors of the new job.
    jobs_with_out_symbol: HashMap<String, HashSet<i64>>,
}

impl State {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            jobs_with_out_symbol: HashMap::new(),
        }
    }

    pub fn add_in_dependency(
        &mut self,
        in_job: &mut Job,
        symbol: &String,
    ) -> Result<(), StateError> {
        /*! Look for previously submitted jobs whose `out` symbol matches the
         * `in` symbol of the provided job.  For each matching job: add the new
         * job as a child of the previously submitted job and add the previously
         * submitted job as an ancestor of the new job.  If no matches are
         * found, do nothing (i.e., the job should be free to be scheduled
         * immediately)
         */
        match self.jobs_with_out_symbol.get(symbol) {
            Some(out_jobs) => {
                for out_jobid in out_jobs.iter() {
                    let out_job: &mut Job = match self.jobs.get_mut(out_jobid) {
                        Some(x) => x,
                        None => {
                            return Err(StateError::InvalidJobID)
                        }
                    };
                    in_job.ancestors.insert(out_job.jobid);
                    out_job.children.insert(in_job.jobid);
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub fn add_out_dependency(&mut self, out_job: &Job, symbol: &String) {
        //! Add the new job to the appropriate key in the `jobs_out_symbol`
        //! lookup table, where the "appropriate key" is the `out` symbol.
        match self.jobs_with_out_symbol.get_mut(symbol) {
            Some(out_jobs) => {
                out_jobs.insert(out_job.jobid);
            }
            None => {
                self.jobs_with_out_symbol
                    .insert(symbol.clone(), vec![out_job.jobid].into_iter().collect());
            }
        };
    }

    pub fn rollback_job_add(&mut self, job: &Job) {}

    pub fn add_job(&mut self, jobid: i64, dependencies: &Vec<Dependency>) {
        /*! Add a new job into the dependency graph, considering all of its
         * dependencies.  For each dependency of the new job, InOut
         * dependencies are broken down into an `In` insertion followed by an
         * an `Out` insertion.
         */
        if dependencies.len() == 0 {
            return;
        }

        let mut job = Job::new(jobid);

        for dependency in dependencies.iter() {
            let result = match dependency.dep_type {
                DependencyType::In => self.add_in_dependency(&mut job, &dependency.value),
                DependencyType::Out => {
                    self.add_out_dependency(&mut job, &dependency.value);
                    Ok(())
                }
                DependencyType::InOut => {
                    let ret = self.add_in_dependency(&mut job, &dependency.value);
                    self.add_out_dependency(&mut job, &dependency.value);
                    ret
                }
            };
            match result {
                Ok(()) => (),
                Err(e) => {
                    eprintln!("Failed to add dependency for job: {:?}", e);
                    self.rollback_job_add(&job);
                }
            }
        }

        self.jobs.insert(jobid, job);
    }

    pub fn job_event(&mut self, jobid: i64, event: String) -> Result<HashSet<i64>, StateError> {
        /*! Given a specific job and it's event, calculate the effects of this
         * event on other jobs.  Specifically, calculate which jobs are now
         * free to run. For example, if a job completes, determine which jobs
         * are now free to run given the completion of their dependency.
         */
        let job = match self.jobs.get(&jobid) {
            Some(x) => x,
            None => return Err(StateError::InvalidJobID),
        };
        let mut ret = HashSet::new();
        match event.as_str() {
            "submit" => {
                if job.ancestors.len() == 0 {
                    ret.insert(jobid);
                }
            }
            "depend" => {},
            "alloc" => {},
            "finish" => {
                let mut error_occurred : bool = false;
                let error : StateError = StateError::MissingDescendent;
                for child_id in job.children.clone().iter() {
                    let child_job = match self.jobs.get_mut(child_id) {
                        Some(x) => x,
                        None => {
                            eprintln!("Child Job ID ({}) not found", child_id);
                            error_occurred = true;
                            continue;
                        },
                    };
                    if !child_job.ancestors.remove(&jobid) {
                        eprintln!("WARN: Job ID not found in child's ancestors");
                    }
                    if child_job.ancestors.len() == 0 {
                        ret.insert(child_job.jobid);
                    }
                }
                if error_occurred {
                    return Err(error);
                }
            }
            _ => {return Err(StateError::InvalidEvent)}
        }
        Ok(ret)
    }
}

enum DependencyType {
    In,
    Out,
    InOut,
}

enum DependencyScope {
    User,
    Global,
}

enum DependencyScheme {
    String,
    Fluid,
}

struct Dependency {
    dep_type: DependencyType,
    scope: DependencyScope,
    scheme: DependencyScheme,
    value: String,
}

impl Dependency {
    pub fn new(dep_type: DependencyType, value: String) -> Self {
        Self {
            dep_type: dep_type,
            scope: DependencyScope::Global,
            scheme: DependencyScheme::String,
            value: value,
        }
    }
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_noop(actual: Result<HashSet<i64>, StateError>) {
        assert!(actual.is_ok());
        assert_eq!(actual.unwrap().len(), 0)
    }

    fn assert_jobs_eq(actual: Result<HashSet<i64>, StateError>, expected: &Vec<i64>) {
        assert!(actual.is_ok());
        assert_eq!(
            actual.unwrap(),
            HashSet::from_iter(expected.iter().cloned())
        );
    }

    fn assert_err_eq(actual: Result<HashSet<i64>, StateError>, expected: StateError) {
        // replace with `contains_err` once it is stable
        assert!(actual.is_err());
        assert_eq!(
            actual.unwrap_err(),
            expected,
        );
    }

    #[test]
    fn job_chain() {
        let mut state = State::new();

        state.add_job(
            1,
            &vec![Dependency::new(DependencyType::Out, "foo".to_string())],
        );
        state.add_job(
            2,
            &vec![Dependency::new(DependencyType::InOut, "foo".to_string())],
        );
        state.add_job(
            3,
            &vec![Dependency::new(DependencyType::In, "foo".to_string())],
        );

        // Submit all the things!
        let out = state.job_event(1, "submit".to_string());
        assert_jobs_eq(out, &vec![1]);
        for jobid in vec![2, 3].iter() {
            let out = state.job_event(*jobid, "submit".to_string());
            assert_noop(out);
        }

        for jobid in vec![1, 2, 3].iter() {
            let out = state.job_event(*jobid, "depend".to_string());
            assert_noop(out);
            let out = state.job_event(*jobid, "alloc".to_string());
            assert_noop(out);
            let out = state.job_event(*jobid, "finish".to_string());
            if *jobid < 3 {
                assert_jobs_eq(out, &vec![jobid + 1]);
            } else {
                assert_noop(out);
            }
        }
    }

    #[test]
    fn job_fan_out() {
        let mut state = State::new();
        state.add_job(
            1,
            &vec![Dependency::new(DependencyType::Out, "foo".to_string())],
        );
        for jobid in vec![2, 3, 4].iter() {
            state.add_job(
                *jobid,
                &vec![
                    Dependency::new(DependencyType::In, "foo".to_string()),
                    Dependency::new(DependencyType::Out, "bar".to_string()),
                ],
            );
        }
        state.add_job(
            5,
            &vec![Dependency::new(DependencyType::In, "bar".to_string())],
        );

        // Submit all the things!
        let out = state.job_event(1, "submit".to_string());
        assert_jobs_eq(out, &vec![1]);
        for jobid in vec![2, 3, 4, 5].iter() {
            let out = state.job_event(*jobid, "submit".to_string());
            assert_noop(out);
        }

        // Run and complete initial pre-process job
        let out = state.job_event(1, "depend".to_string());
        assert_noop(out);
        let out = state.job_event(1, "alloc".to_string());
        assert_noop(out);
        let out = state.job_event(1, "finish".to_string());
        assert_jobs_eq(out, &vec![2, 3, 4]);

        // Run and complete fan-out
        for jobid in vec![2, 3, 4].iter() {
            let out = state.job_event(*jobid, "depend".to_string());
            assert_noop(out);
            let out = state.job_event(*jobid, "alloc".to_string());
            assert_noop(out);
            let out = state.job_event(*jobid, "finish".to_string());
            if *jobid < 4 {
                assert_noop(out);
            } else {
                assert_jobs_eq(out, &vec![5]);
            }
        }

        // Run and complete postprocess job
        let out = state.job_event(5, "depend".to_string());
        assert_noop(out);
        let out = state.job_event(5, "alloc".to_string());
        assert_noop(out);
        let out = state.job_event(5, "finish".to_string());
        assert_noop(out);
    }

    #[test]
    fn nonexistent_in() {
        //! A job with an 'in' dependency that does not match an 'out' of a
        //! currently queued/running job can be immediately scheduled.
        let mut state = State::new();
        state.add_job(
            1,
            &vec![Dependency::new(DependencyType::In, "foo".to_string())],
        );
        let out = state.job_event(1, "submit".to_string());
        assert_jobs_eq(out, &vec![1]);
    }

    #[test]
    fn invalid_jobid() {
        //! Test that an event on an unknown/invalid returns an error
        let mut state = State::new();
        let out = state.job_event(1, "submit".to_string());
        assert_err_eq(out, StateError::InvalidJobID);
        state.add_job(
            1,
            &vec![Dependency::new(DependencyType::In, "foo".to_string())],
        );
        let out = state.job_event(1, "foobar".to_string());
        assert_err_eq(out, StateError::InvalidEvent);
    }
}
