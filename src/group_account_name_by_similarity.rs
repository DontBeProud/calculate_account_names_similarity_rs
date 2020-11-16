use std::collections::HashSet;
use crate::analyze_account_name_similarity::{CAccountNameSimAnalyse, CAccountNameSimResultDetail};
use std::cmp::min;


// 用于对账号集合进行分组的判定阈值
pub struct CSimilarityGroupingThreshold{
    pub threshold_sim: f64,                 // 相似度阈值,高于等于这个阈值则判定两个账号相似,可以被分为一组
    pub threshold_frequency: f64,           // 频率阈值,即某组的成员占所在账号块总帐号(划分后的某一账号块,非所有账号)的比例,数值高低需要根据应用场景决定,过低会影响效率
    pub threshold_integrate: f64,           // 整合阈值,用于将多线程运算的结果整合,此值过高可能导致本应属于一个账号组的账号被划分至多组,过低会导致本应划分为多组的账号被划分入一组
    pub threshold_group_members: f64        // 组员数量阈值,越低越准确,但是过高会影响运行效率
}
impl CSimilarityGroupingThreshold{
    pub fn default() -> CSimilarityGroupingThreshold{
        CSimilarityGroupingThreshold{
            threshold_sim: 0.856,
            threshold_frequency: 0.005,
            threshold_integrate: 0.856,
            threshold_group_members: 10.0
        }
    }
    pub fn set_threshold_sim(&mut self, threshold_sim: f64){
        self.threshold_sim = threshold_sim;
    }
    pub fn set_threshold_frequency(&mut self, threshold_frequency: f64){
        self.threshold_frequency = threshold_frequency;
    }
    pub fn set_threshold_integrate(&mut self, threshold_integrate: f64){
        self.threshold_integrate = threshold_integrate;
    }
    pub fn set_threshold_group_members(&mut self, threshold_group_members: f64){
        self.threshold_group_members = threshold_group_members;
    }
}


// 用于对账号集合排序/分组
pub struct CAccountNameAnaVec<'a>{
    pub analyse_obj_vec: Vec<CAccountNameSimAnalyse<'a>>,
    pub data_vec_size: usize
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
        CAccountNameAnaVec{ data_vec_size: obj_vec.len(), analyse_obj_vec: obj_vec}
    }


    // 基于相似度对账号名称进行分组
    // threshold: 阈值
    // group_granularity: 分割粒度
    // 根据分割粒度将数据分割成若干数据块，并分别交由子线程处理,最后进行数据汇总
    // 设置合理的分割粒度可以在保证准确性的基础上提高运算效率
    pub fn group_account_names_by_similarity(&self, threshold: &CSimilarityGroupingThreshold, group_granularity: usize){
    }


    // 根据分组粒度对数据进行分组
    fn split_data_vec_by_granularity(&self, group_granularity: usize) -> Vec<Vec<CAccountNameSimAnalyse<'a>>>{
        let mut result: Vec<Vec<CAccountNameSimAnalyse<'a>>>= Vec::new();
        let real_group_granularity = min(group_granularity, self.data_vec_size);

        for i in 0..(self.data_vec_size / real_group_granularity){
            result.push(Vec::new());
            result[i] = (&self.analyse_obj_vec[i * real_group_granularity..((i + 1) * real_group_granularity)].to_vec()).clone();
        }

        // 若最后一组的数据成员过少(数量少于real_group_granularity / 2)，则合并入前一组
        let res_len = result.len();
        let remaining_data_len = self.data_vec_size - res_len * real_group_granularity;
        if remaining_data_len > 0 {
            if (remaining_data_len as f64) < (real_group_granularity as f64 / 2.0){
                result[res_len - 1].append(&mut (&self.analyse_obj_vec[res_len * real_group_granularity..].to_vec()).clone());
            }
            else {
                result.push(Vec::new());
                result[res_len] = (&self.analyse_obj_vec[res_len * real_group_granularity..].to_vec()).clone();
            }
        }

        result
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        let vec_obj = vec!["a1f6", "aa11ff66", "b2c", "a1f55", "1"];
        let tmp = CAccountNameAnaVec::new(&vec_obj);
        // tmp.split_data_vec_by_granularity(1);
        // for i in tmp.split_data_vec_by_granularity(7){
        //     println!("{:?}", i);
        }


    }
}