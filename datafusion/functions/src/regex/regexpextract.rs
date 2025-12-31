// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Regex expressions
use crate::regex::regexpmatch::regexp_match;
use arrow::array::{Array, ArrayRef, AsArray, GenericStringBuilder};
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, Documentation, TypeSignature};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Returns the request group id [regular expression](https://docs.rs/regex/latest/regex/#syntax) matche in a string.",
    syntax_example = "regexp_extract(str, regexp[, flags], idx)",
    sql_example = r#"```sql
            > select regexp_extract('Köln', '([a-zA-Z])ö([a-zA-Z]){2}', 2);
            +------------------------------------------------------------------+
            | regexp_extract(Utf8("Köln"),Utf8("([a-zA-Z])ö([a-zA-Z]){2}"), 2) |
            +------------------------------------------------------------------+
            | [ln]                                                             |
            +------------------------------------------------------------------+
```
"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "regexp",
        description = "Regular expression to match against.
            Can be a constant, column, or function."
    ),
    argument(
        name = "flags",
        description = r#"Optional regular expression flags that control the behavior of the regular expression. The following flags are supported:
  - **i**: case-insensitive: letters match both upper and lower case
  - **m**: multi-line mode: ^ and $ match begin/end of line
  - **s**: allow . to match \n
  - **R**: enables CRLF mode: when multi-line mode is enabled, \r\n is used
  - **U**: swap the meaning of x* and x*?"#
    ),
    argument(
        name = "idx",
        description = "group index to retrive."
    )
)]

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpExtractFunc {
    signature: Signature,
}

impl Default for RegexpExtractFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpExtractFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    // Planner attempts coercion to the target type starting with the most preferred candidate.
                    // For example, given input `(Utf8View, Utf8)`, it first tries coercing to `(Utf8View, Utf8View)`.
                    // If that fails, it proceeds to `(Utf8, Utf8)`.
                    TypeSignature::Exact(vec![Utf8View, Utf8View]),
                    TypeSignature::Exact(vec![Utf8, Utf8]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8]),
                    TypeSignature::Exact(vec![Utf8View, Utf8View, Utf8View]),
                    TypeSignature::Exact(vec![Utf8, Utf8, Utf8]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpExtractFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match &arg_types[0] {
            DataType::Null => DataType::Null,
            other => DataType::List(Arc::new(Field::new_list_field(other.clone(), true))),
        })
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        // FixMe: extract idx from the arguments
        let idx = 0;
        // and FixMe

        let is_scalar = len.is_none();
        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .map(|arg| arg.to_array(inferred_length))
            .collect::<Result<Vec<_>>>()?;

        let result = regexp_extract(&args, idx);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}


pub fn regexp_extract(args: &[ArrayRef], idx : usize ) -> Result<ArrayRef> {
    // [[text], [pattern], [flags]], idx
    let params = if args.len() == 3 {&args[0..3]} else {&args[0..2]};
    let re = regexp_match(params)?;
    let mut builder: GenericStringBuilder<i32> = GenericStringBuilder::new();

    let list_array = re.as_list::<i32>();
    for i in 0..list_array.len() {
        if list_array.is_null(i) {
            println!("Row {}: No match found", i);
            builder.append_null();
            continue;
        }

        // 3. Each row contains an array of capturing groups
        // Downcast the inner value to a StringArray
        let inner_array = list_array.value(i);
        let captures = inner_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Inner captures should be StringArray");

        print!("Row {}: ", i);
        for j in 0..captures.len() {
            let capture_group = captures.value(j);
            print!("Group[{}]: '{}' ", j, capture_group);
        }
        println!();

        if idx < captures.len() {
            builder.append_value(captures.value(idx));
            println!("builder appended: {}", captures.value(idx))
        } else {
            builder.append_null();
            println!("builder appended null");
        }
    }

    let ret : ArrayRef = Arc::new(builder.finish()); 
    return Ok(ret);
}

#[cfg(test)]
mod tests {
    use crate::regex::regexpextract::regexp_extract;
    use arrow::array::StringArray;
    use arrow::array::{GenericStringBuilder};
    use std::sync::Arc;

    #[test]
    fn test_case_sensitive_regexp_extract() {
        let values = StringArray::from(vec!["abc"; 2]);
        let patterns = StringArray::from(vec!["^(a)(b)", "^(b)"]);
        let group_id = 1;
            
        let re = regexp_extract(&[Arc::new(values), Arc::new(patterns)], group_id)
            .unwrap();

        println!("regexp_extract returned:\n{:?}", re);

        let mut expected_builder: GenericStringBuilder<i32> = GenericStringBuilder::new();

        expected_builder.append_value("b");
        expected_builder.append_null();

        let expected = expected_builder.finish();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_case_insensitive_regexp_extract() {
        let values = StringArray::from(vec!["abc"; 2]);
        let patterns = StringArray::from(vec!["^(A)(b)", "^(b)"]);
        let flags = StringArray::from(vec!["i"; 2]);
        let group_id = 0;
            
        let re = regexp_extract(&[Arc::new(values), Arc::new(patterns), Arc::new(flags)], group_id)
            .unwrap();

        println!("regexp_extract returned:\n{:?}", re);

        let mut expected_builder: GenericStringBuilder<i32> = GenericStringBuilder::new();

        expected_builder.append_value("a");
        expected_builder.append_null();

        let expected = expected_builder.finish();

        assert_eq!(re.as_ref(), &expected);
    }
}
