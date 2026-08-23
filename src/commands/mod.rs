mod aggregate;
mod cast;
mod collect;
mod count;
mod describe;
mod distinct;
mod drop;
mod explain;
mod filter;
mod from_nu;
mod join;
mod limit;
mod open;
mod register;
mod rename;
mod sample;
mod save;
mod schema;
mod select;
mod sort;
mod sql;
mod stub;
mod tables;
mod union;
mod unregister;
mod with_column;

use nu_plugin::PluginCommand;
use nu_protocol::Category;

use crate::plugin::DfPlugin;

pub fn category() -> Category {
    Category::Custom("dataframe".into())
}

pub fn all() -> Vec<Box<dyn PluginCommand<Plugin = DfPlugin>>> {
    vec![
        Box::new(stub::DfCmd),
        Box::new(open::Open),
        Box::new(save::Save),
        Box::new(sql::Sql),
        Box::new(register::Register),
        Box::new(unregister::Unregister),
        Box::new(tables::Tables),
        Box::new(schema::SchemaCmd),
        Box::new(collect::Collect),
        Box::new(count::Count),
        Box::new(explain::Explain),
        Box::new(describe::Describe),
        Box::new(sample::Sample),
        Box::new(from_nu::FromNu),
        Box::new(select::Select),
        Box::new(filter::Filter),
        Box::new(with_column::WithColumn),
        Box::new(drop::DropCols),
        Box::new(sort::Sort),
        Box::new(limit::Limit),
        Box::new(join::Join),
        Box::new(aggregate::Aggregate),
        Box::new(distinct::Distinct),
        Box::new(union::Union),
        Box::new(rename::Rename),
        Box::new(cast::Cast),
    ]
}
