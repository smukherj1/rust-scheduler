#[derive(Debug, Default)]
pub struct Queue {}

impl Queue {
    pub fn new() -> Self {
        Queue {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_succeeds() {
        Queue::new();
    }
}
