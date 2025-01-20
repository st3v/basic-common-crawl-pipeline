use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use arrow::array::{ArrayRef, ListArray, ListBuilder, RecordBatch, StringArray, StringBuilder};
use std::sync::Arc;

pub fn encode(ids: Vec<&str>, urls: Vec<&str>, timestamps: Vec<&str>, token_lists: Vec<Vec<&str>>) -> Vec<u8> {
    let ids = StringArray::from(ids);
    let urls = StringArray::from(urls);
    let timestamps = StringArray::from(timestamps);
    let token_lists = build_list_array(token_lists);
    let batch = RecordBatch::try_from_iter(vec![
      ("id", Arc::new(ids) as ArrayRef),
      ("url", Arc::new(urls) as ArrayRef),
      ("timestamp", Arc::new(timestamps) as ArrayRef),
      ("tokens", Arc::new(token_lists) as ArrayRef),
    ]).unwrap();
   
    let mut buf = Vec::new();
   
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
   
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).expect("Writing batch");
    writer.close().unwrap();
    buf
}

fn build_list_array(lists: Vec<Vec<&str>>) -> ListArray {
    let values_builder = StringBuilder::new();
    let mut builder = ListBuilder::new(values_builder);
    for list in lists {
        for value in list {
            builder.values().append_value(value);
        }
        builder.append(true);
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_list_array() {
        let lists = vec![vec!["hello", "world", "!"], vec![], vec!["foo", "bar"]];
        let list_array = super::build_list_array(lists);

        // Todo: find a way to assert the list array
        eprintln!("{:?}", list_array);
    }

    #[test]
    fn test_encode() {
        let ids = vec!["1", "2"];
        let urls = vec!["http://foo.org", "http://bar.org"];
        let timestamps = vec![ "2021-01-01T00:00:00Z", "2021-01-01T00:00:00Z"];
        let token_lists = vec![vec!["hello", "foo"], vec!["hello", "bar"]];
        let _ = encode(ids, urls, timestamps, token_lists);

        // Todo: find a way to assert the list array
        // std::fs::write("test.parquet", &buf).unwrap();
    }
}