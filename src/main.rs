use std::collections::HashMap;

struct State {
    deps: HashMap<i64, i64>,
}

impl State {
    pub fn new() -> Self {
        Self {
            deps: HashMap::new(),
        }
    }
}

enum DependencyType {
    In,
    Out,
    InOut,
}

struct Dependency {
    dep_type: DependencyType,
    label: String,
}

impl Dependency {
    pub fn new(dep_type: DependencyType, label: String) -> Self {
        Self {
            dep_type: dep_type,
            label: label,
        }
    }
}

fn new_job(state: &mut State, jobid: i64, dependencies: &Vec<Dependency>) {
    
}

fn job_event(state: &mut State, jobid: i64, event: String) -> Vec<i64> {
    Vec::new()
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_noop(vec: &Vec<i64>) {
        assert_eq!(vec.len(), 0)
    }

    #[test]
    fn job_chain() {
        let mut state = State::new();

        new_job(&mut state, 1, &vec![Dependency::new(DependencyType::Out, "foo".to_string())]);
        new_job(&mut state, 2, &vec![Dependency::new(DependencyType::InOut, "foo".to_string())]);
        new_job(&mut state, 3, &vec![Dependency::new(DependencyType::In, "foo".to_string())]);

        // Submit all the things!
        let mut out = job_event(&mut state, 1, "submit".to_string());
        assert_eq!(out, vec![1]);
        for jobid in vec![2, 3].iter() {
            out = job_event(&mut state, *jobid, "submit".to_string());
            assert_noop(&out);
        }

        for jobid in vec![1, 2, 3].iter() {
            out = job_event(&mut state, *jobid, "depend".to_string());
            assert_noop(&out);
            out = job_event(&mut state, *jobid, "alloc".to_string());
            assert_noop(&out);
            out = job_event(&mut state, *jobid, "finish".to_string());
            if *jobid < 3 {
                assert_eq!(out, vec![jobid + 1]);
            } else {
                assert_noop(&out);
            }
        }
    }

    #[test]
    fn job_fan_out() {
        let mut state = State::new();
        new_job(&mut state, 1, &vec![Dependency::new(DependencyType::Out, "foo".to_string())]);
        for jobid in vec![2, 3, 4].iter() {
            new_job(&mut state, *jobid, &vec![Dependency::new(DependencyType::In, "foo".to_string()), Dependency::new(DependencyType::Out, "bar".to_string())]);
        }
        new_job(&mut state, 5, &vec![Dependency::new(DependencyType::In, "bar".to_string())]);

        // Submit all the things!
        let mut out = job_event(&mut state, 1, "submit".to_string());
        assert_eq!(out, vec![1]);
        for jobid in vec![2, 3, 4, 5].iter() {
            out = job_event(&mut state, *jobid, "submit".to_string());
            assert_noop(&out);
        }

        // Run and complete initial pre-process job
        out = job_event(&mut state, 1, "depend".to_string());
        assert_noop(&out);
        out = job_event(&mut state, 1, "alloc".to_string());
        assert_noop(&out);
        out = job_event(&mut state, 1, "finish".to_string());
        assert_eq!(out, vec![2, 3, 4]);

        // Run and complete fan-out
        for jobid in vec![2, 3, 4].iter() {
            out = job_event(&mut state, *jobid, "depend".to_string());
            assert_noop(&out);
            out = job_event(&mut state, *jobid, "alloc".to_string());
            assert_noop(&out);
            out = job_event(&mut state, *jobid, "finish".to_string());
            if *jobid < 4 {
                assert_noop(&out);
            } else {
                assert_eq!(out, vec![5]);
            }
        }

        // Run and complete postprocess job
        out = job_event(&mut state, 5, "depend".to_string());
        assert_noop(&out);
        out = job_event(&mut state, 5, "alloc".to_string());
        assert_noop(&out);
        out = job_event(&mut state, 5, "finish".to_string());
        assert_noop(&out);
    }
}
