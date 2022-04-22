use mbei_core::graph::Delta;
use crate::producer::TopicNameAndUpdate;

pub trait MessageCreator {
     fn create_one_new_message(
        &mut self,
        stopping: bool,
    ) -> (Vec<Delta>, Vec<TopicNameAndUpdate>);

    fn is_finished(
        &self
    ) -> bool;

    fn get_current_timestamp(&self) -> u64;
}