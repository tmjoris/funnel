//! `quantile(sketch, q)`: a SQL aggregate that merges stored DDSketches and reads a quantile.
//!
//! Merging is what makes this correct. A p99 across hosts or across hours comes from merging the
//! sketches, which averaging per-bucket p99s cannot reproduce.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{Accumulator, AggregateUDF, Volatility, create_udaf};
use funnel_sketch::DDSketch;

pub fn udaf() -> AggregateUDF {
    create_udaf(
        "quantile",
        vec![DataType::LargeBinary, DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(Quantile::default()))),
        Arc::new(vec![DataType::LargeBinary, DataType::Float64]),
    )
}

#[derive(Debug, Default)]
struct Quantile {
    sketch: Option<DDSketch>,
    q: Option<f64>,
}

impl Quantile {
    fn add_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let incoming = match DDSketch::decode(bytes) {
            Ok((sketch, _)) => sketch,
            Err(e) => return exec_err!("bad sketch: {e:?}"),
        };
        match &mut self.sketch {
            Some(acc) => {
                if let Err(e) = acc.merge(&incoming) {
                    return exec_err!("sketches do not merge: {e}");
                }
            }
            None => self.sketch = Some(incoming),
        }
        Ok(())
    }

    fn take(&mut self, sketches: &ArrayRef, qs: &ArrayRef) -> Result<()> {
        let sketches = sketches.as_binary::<i64>();
        let qs = qs.as_primitive::<Float64Type>();
        for i in 0..sketches.len() {
            if !qs.is_null(i) {
                self.q = Some(qs.value(i));
            }
            if !sketches.is_null(i) {
                self.add_bytes(sketches.value(i))?;
            }
        }
        Ok(())
    }
}

impl Accumulator for Quantile {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.take(&values[0], &values[1])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.take(&states[0], &states[1])
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::LargeBinary(self.sketch.as_ref().map(DDSketch::to_bytes)),
            ScalarValue::Float64(self.q),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = match (&self.sketch, self.q) {
            (Some(s), Some(q)) => s.quantile(q),
            _ => None,
        };
        Ok(ScalarValue::Float64(value))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.sketch.as_ref().map_or(0, DDSketch::memory_bytes)
    }
}
