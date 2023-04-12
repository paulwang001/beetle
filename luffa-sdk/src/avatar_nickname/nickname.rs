use std::io::{BufReader, Cursor};
use std::ops::Rem;
use calamine::{Reader, Xlsx,open_workbook};
use once_cell::sync::Lazy;
use crate::avatar_nickname::hash::my_hash;
use std::ops::Div;

static ALL_NAMES: Lazy<(Vec<String>, Vec<String>)> = Lazy::new(||{
    let data = include_bytes!("../../names.xlsx");
    let cursor = Cursor::new(data);
    let reader = BufReader::new(cursor);
    let mut excel: Xlsx<_> =Reader::new(reader).unwrap();


    let mut adjs: Vec<String> =  Vec::new();
    let mut vegetables: Vec<String> =  Vec::new();

    let adj_sheet_name = "adj";
    let vegetable_sheet_name = "vegetable";



    for sheet_name in excel.sheet_names().to_owned() {
        let range = excel
            .worksheet_range(&sheet_name)
            .ok_or(format!("Error reading sheet {}", sheet_name)).unwrap().unwrap();

        for row in range.rows () {
            for cell in row.iter() {
                match cell {
                    calamine::DataType::String(s) => {
                        if sheet_name.contains(adj_sheet_name){
                            adjs.push(s.to_string());
                        }
                        if sheet_name.contains(vegetable_sheet_name){
                            vegetables.push(s.to_string());
                        }
                        // print!("{} ", s)
                    },
                    _ =>{}

                }
            }
        }

    }
    (adjs, vegetables)
});

pub fn generate_nickname(peer_id: &str) -> String {
    let peer_id = my_hash(peer_id.as_bytes());
    let left = ALL_NAMES.0.len();
    let right = ALL_NAMES.1.len();
    let res_name_index = (peer_id as usize).rem(left * right);
    let left = res_name_index.div(right);
    let right = res_name_index.rem(right);
    format!("{} {}",ALL_NAMES.0.get(left).unwrap(), ALL_NAMES.1.get(right).unwrap() )
}