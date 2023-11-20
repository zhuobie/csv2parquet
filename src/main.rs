use arrow::csv::ReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::{fs::File, error::Error};
use std::path::PathBuf;
use std::sync::Arc;
use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Opts {
    #[arg(short, long, help = "input csv file")]
    input: Option<PathBuf>,
    #[arg(short, long, help = "optional, output parquet file")]
    output: Option<PathBuf>,
}

fn csv2parquet(csv: &PathBuf, parquet: &PathBuf) -> Result<(), Box<dyn Error>> {
    let mut input = File::open(csv)?;
    let (schema, _) = arrow::csv::reader::infer_file_schema(&mut input, ',' as u8, Some(100), true)?;
    let schema_ref = Arc::new(schema);
    let builder = ReaderBuilder::new().has_header(true).with_delimiter(',' as u8).with_schema(schema_ref);
    let reader = builder.build(input)?;
    let output = File::create(parquet)?;
    let mut writer = ArrowWriter::try_new(output, reader.schema(), None)?;
    for batch in reader {
        let batch = batch?;
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let opts = Opts::parse();    
    if let Some(input) = opts.input {
        if let Some(output) = opts.output {
            csv2parquet(&input, &output)?;
        } else {
            csv2parquet(
                &input, 
                &input.with_file_name(&input.file_stem().unwrap().to_str().unwrap()).with_extension("parquet")
            )?;
        }
    } else {
        println!("Please input csv file path.");
    }
    Ok(())
}
