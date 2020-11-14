use std::collections::HashSet;
use crate::analyze_account_name_similarity::{CAccountNameSimAnalyse, CAccountNameSimResultDetail};

// 用于对账号集合排序/分组
pub struct CAccountNameAnaVec<'a>{
    pub analyse_obj_vec: Vec<CAccountNameSimAnalyse<'a>>,
}
impl<'a> CAccountNameAnaVec<'a>{
    pub fn new(account_name_vec: &'a Vec<&str>) -> CAccountNameAnaVec<'a> {
        let mut obj_vec: Vec<CAccountNameSimAnalyse<'a>> = Vec::new();

        // 去重
        let account_name_hash_set: HashSet<&&str> = account_name_vec.into_iter().collect();
        for &item in account_name_hash_set{
            obj_vec.push(CAccountNameSimAnalyse::new(item));
        }

        // 排序
        obj_vec.sort_by_key(|k|
            (k.skeleton_style.to_vec(), k.skeleton_part_size_list.to_vec(), k.account_name.to_string()));
        CAccountNameAnaVec{ analyse_obj_vec: obj_vec}
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        let vec_obj = vec!["a1f6", "aa11ff66", "b2c", "a1f55"];
        let tmp = CAccountNameAnaVec::new(&vec_obj);
        println!("{:?}", tmp.analyse_obj_vec);
    }
}