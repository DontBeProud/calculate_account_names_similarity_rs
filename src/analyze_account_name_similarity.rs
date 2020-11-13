#[macro_use]
use lazy_static::lazy_static;
use crate::algorithm::{calc_edit_distance, calc_jaro_winkler_distance, calc_similarity_between_digits,
                       calc_similarity_between_i64vecs, calc_similarity_between_vvecs, split_account_name_by_data_type};

// 相似度细节
#[derive(Debug)]
pub struct CAccountNameSimDetail{
    pub sim_total_score: f64,
    pub sim_score: f64,
    pub sim_jaro_distance: f64,
    pub sim_edit_distance: i64,
    pub sim_length: f64,
    pub sim_item_list: f64,
    pub sim_item_amount: f64,
    pub sim_skeleton_style: f64,
    pub sim_skeleton_part_size_list: f64
}

// 量化相似度过程中各参数的权重表
pub struct CAccountNameSimAnalyseParamsWeightTable{
    pub skeleton_skeleton_style: i64,
    pub skeleton_part_size_list: i64,
    pub length:  i64,
    pub item_list: i64,
    pub item_amount: i64,
}

// 对相似度量化计算的封装
pub struct CAccountNameSimAnalyse<'a>{
    pub account_name: &'a str,
    pub length: i64,
    pub item_list:  Vec<Vec<i64>>,
    pub item_amount: i64,
    pub skeleton_style: Vec<i64>,
    pub skeleton_part_size_list: Vec<i64>
}

// 默认参数权重表
lazy_static! {
    static ref DEFAULT_PARAMETER_WEIGHT_TABLE: CAccountNameSimAnalyseParamsWeightTable =
    CAccountNameSimAnalyseParamsWeightTable{
        skeleton_skeleton_style: 7,
        skeleton_part_size_list: 9,
        length:  1,
        item_list: 8,
        item_amount: 3,
    };
}

impl Default for CAccountNameSimDetail {
    fn default() -> Self {
        CAccountNameSimDetail {
            sim_total_score: 0.0,
            sim_score: 0.0,
            sim_jaro_distance: 0.0,
            sim_edit_distance: 0,
            sim_length: 0.0,
            sim_item_list: 0.0,
            sim_item_amount: 0.0,
            sim_skeleton_style: 0.0,
            sim_skeleton_part_size_list: 0.0
        }
    }
}

impl<'a> CAccountNameSimAnalyse<'a>{

    // init
    pub fn new(_account_name: &'a str) ->CAccountNameSimAnalyse{
        let split_res = split_account_name_by_data_type(_account_name);
        CAccountNameSimAnalyse{
            account_name: _account_name,
            length: _account_name.len() as i64,
            item_amount: split_res.0.len() as i64,
            item_list: split_res.0,
            skeleton_style: split_res.1,
            skeleton_part_size_list: split_res.2,
        }
    }

    // 计算编辑距离
    fn analyze_edit_distance(&self, account_name: &str) -> i64{
        calc_edit_distance(self.account_name, account_name) as i64
    }

    // 计算jaro-winkler相似度
    fn analyze_jaro_distance(&self, account_name: &str) -> f64{
        calc_jaro_winkler_distance(self.account_name, account_name)
    }

    // 计算零件集合相似度
    fn analyze_similarity_item_list(&self, item_list: &Vec<Vec<i64>>) -> f64{
        calc_similarity_between_vvecs(&self.item_list, item_list)
    }

    // 计算骨架零件相似度
    fn analyze_similarity_skeleton_part_size_list(&self, item_list: &Vec<i64>) -> f64 {
        calc_similarity_between_i64vecs(&self.skeleton_part_size_list, item_list)
    }

    // 计算骨架相似度
    fn analyze_similarity_skeleton_style(&self, skeleton: &Vec<i64>) -> f64{
        calc_similarity_between_i64vecs(&self.skeleton_style, skeleton)
    }

    // 计算零件数量相似度
    fn analyze_similarity_item_amount(&self, item_amount: i64) -> f64{
        calc_similarity_between_digits(self.item_amount, item_amount)
    }

    // 计算字符串长度相似度
    fn analyze_similarity_length(&self, length: i64) -> f64{
        calc_similarity_between_digits(self.length, length)
    }

    // 计算两个账号名称的相似度（需要传入参数权重表）
    fn calc_similarity_by_specify_param_weights(&self,
                                                obj_to_cmp: &CAccountNameSimAnalyse,
                                                weight_table: &CAccountNameSimAnalyseParamsWeightTable) -> (f64, CAccountNameSimDetail){

        let mut ret_detail: CAccountNameSimDetail = Default::default();

        ret_detail.sim_length = self.analyze_similarity_length(obj_to_cmp.length);
        ret_detail.sim_item_amount = self.analyze_similarity_item_amount(obj_to_cmp.item_amount);
        ret_detail.sim_skeleton_style = self.analyze_similarity_skeleton_style(&obj_to_cmp.skeleton_style);
        ret_detail.sim_skeleton_part_size_list = self.analyze_similarity_skeleton_part_size_list(&obj_to_cmp.skeleton_part_size_list);
        ret_detail.sim_item_list = self.analyze_similarity_item_list(&obj_to_cmp.item_list);
        ret_detail.sim_edit_distance = self.analyze_edit_distance(obj_to_cmp.account_name);
        ret_detail.sim_jaro_distance = self.analyze_jaro_distance(obj_to_cmp.account_name);
        ret_detail.sim_score = (ret_detail.sim_length * weight_table.length  as f64 +
            ret_detail.sim_item_list * weight_table.item_list  as f64 +
            ret_detail.sim_item_amount * weight_table.item_amount  as f64 +
            ret_detail.sim_skeleton_style * weight_table.skeleton_skeleton_style  as f64 +
            ret_detail.sim_skeleton_part_size_list * weight_table.skeleton_part_size_list  as f64) /
            (weight_table.length + weight_table.item_list + weight_table.item_amount +
                weight_table.skeleton_skeleton_style + weight_table.skeleton_part_size_list)  as f64;
        let average_len: f64 = ((self.account_name.len() + obj_to_cmp.account_name.len()) / 2) as f64;

        ret_detail.sim_total_score = (ret_detail.sim_score * ret_detail.sim_edit_distance as f64 +
            average_len * ret_detail.sim_jaro_distance) /
            (average_len + ret_detail.sim_edit_distance as f64);

        (ret_detail.sim_total_score,  ret_detail)
    }

    // 计算两个账号名称的相似度
    fn calc_similarity(&self, obj_to_cmp: &CAccountNameSimAnalyse) -> (f64, CAccountNameSimDetail){
        self.calc_similarity_by_specify_param_weights(obj_to_cmp, &*DEFAULT_PARAMETER_WEIGHT_TABLE)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        assert_eq!(CAccountNameSimAnalyse::new("u0j2e9u1s2h8l91").analyze_similarity_item_list(&CAccountNameSimAnalyse::new("t9x1h8y0b7g6f42").item_list), 0.23419743655039468);
        assert_eq!(CAccountNameSimAnalyse::new("u0j2e9u1s2h8l91").calc_similarity(&CAccountNameSimAnalyse::new("t9x1h8y0b7g6f42")).0, 0.6072663004595803);
        println!("{:?}", CAccountNameSimAnalyse::new("u0j2e9u1s2h8l91").calc_similarity(&CAccountNameSimAnalyse::new("t9x1h8y0b7g6f42")));
    }
}