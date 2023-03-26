use std::io::{BufReader, Cursor};
use calamine::{Reader, Xlsx,open_workbook};
use once_cell::sync::Lazy;
use crate::avatar_nickname::hash::my_hash;

static EXCEL_BYTES: Lazy<&[u8]> = Lazy::new(||{
    include_bytes!("../../names.xlsx")
});

pub fn generate_nickname(peer_id: &str) -> String {
    let cursor = Cursor::new(EXCEL_BYTES.clone());
    let peer_id = my_hash(peer_id.as_bytes());
    let reader = BufReader::new(cursor);
    let mut excel: Xlsx<_> =Reader::new(reader).unwrap();


    let mut adjs: Vec<String> =  Vec::new();
    let mut vegetables: Vec<String> =  Vec::new();

    let mut all_names: Vec<String> =  Vec::new();

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

    for adj in &adjs{
        for vetetable in &vegetables{
            let name  = format!("{} {}",adj,vetetable);
            // println!("name is {}",name);
            all_names.push(name);
        }
    }

    let res_name_index = peer_id % all_names.len() as u64;
    all_names[res_name_index as usize].clone()
}