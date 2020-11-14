extern crate str_sim;

use std::cmp;
use std::collections::HashMap;
use itertools::Itertools;
use str_sim::{levenshtein_distance, sim_jaro_winkler};



// 计算编辑距离
pub fn calc_edit_distance(s1: &str, s2: &str) -> usize {
    levenshtein_distance(s1, s2)
}


// 计算jaro_winkler距离
pub fn calc_jaro_winkler_distance(s1: &str, s2: &str) -> f64 {
    let prefix_weight = 2.0 / (s1.chars().count() as f64 + s2.chars().count() as f64 + 2.0);
    sim_jaro_winkler(s1, s2, prefix_weight)
}


// 计算两个数字之间的相似度，即对数字之间相差的大小进行量化运算
pub fn calc_similarity_between_digits(dig_a: i64, dig_b: i64) -> f64{
    let min = (cmp::min(dig_a, dig_b) + 1) as f64;
    let max = (cmp::max(dig_a, dig_b) + 1) as f64;
    (min / max) - ((max - min).powf(2.0) / (min.powf(2.0) + max.powf(2.0)))
}


// 分析dif_list, 对其量化打分，各项值越小代表两字符串越相似，计算出的分数越高   例如[0,7,9]的分数高于[5,7,9]
fn calc_score_by_analyze_dif_list(dif_list: &Vec<i64>) -> f64{
    let mut result = 0.0;
    let mut statistics_table: HashMap<i64, i64> = HashMap::new();

    // 生成统计数据映射表
    for i in dif_list{
        // 计数，不存在则创建键值并初始化为1
        *(statistics_table.entry(*i).or_insert(0)) += 1;
    }

    // 平方和公式 : 1^2 + 2^2 + 3^2 + ····· + n^2 = n(n+1)(2n+1)/6
    let list_size = dif_list.len();
    let base_factor = list_size * (list_size + 1) * (2 * list_size + 1) / 6;
    for (key, value) in statistics_table.iter(){
        result += (1 + *value) as f64 / (1 + list_size * *key as usize) as f64
    }
    result * base_factor as f64
}


// 计算两个i64 vec之间的相似度分数     (连续性相似度分析 + 内容相似度分析)
fn calc_similarity_score_between_i64vecs(vec_a: &Vec<i64>, vec_b: &Vec<i64>) -> f64{
    let mut dif_list = Vec::new(); // 存储差值的集合
    let mut count_continuous_same: i64 = 0;  // 连续相同的字符数
    let mut continuity_analysis_result = 0.0000000000001;  //连续性相似度分析
    let min_vec_length = cmp::min(vec_a.len(), vec_b.len());

    for index in 0..min_vec_length{
        let differ = (vec_a[index] - vec_b[index]).abs();
        dif_list.push(differ);

        continuity_analysis_result += (2.0 / (differ + 1) as f64) as f64;
        if differ == 0{
            count_continuous_same += 1;
        }else{
            if count_continuous_same > 1 {
                count_continuous_same -= 1;
            }
        }
        continuity_analysis_result += count_continuous_same.pow(2) as f64;
    }
    continuity_analysis_result + calc_score_by_analyze_dif_list(&dif_list)
}


// 计算两个i64 vec之间的相似度
// 传入&Vec<i64>，若需要计算字符串之间的相似度，则需要逐字符转换成ASCII码，并生成Vec<i64>数据列作为参数传入
pub fn calc_similarity_between_i64vecs(vec_a: &Vec<i64>, vec_b: &Vec<i64>) -> f64 {
    calc_similarity_score_between_i64vecs(vec_a, vec_b) * 2.0 /
        (calc_similarity_score_between_i64vecs(vec_a, vec_a) + calc_similarity_score_between_i64vecs(vec_b, vec_b))
}


// 计算两个Vec<i64> vec之间的相似度分数
fn calc_similarity_score_between_vvecs(vec_a: &Vec<Vec<i64>>, vec_b: &Vec<Vec<i64>>) -> f64 {
    let mut score = 0.0000000000001;
    let iter_times = cmp::min(vec_a.len(), vec_b.len());
    for index in 0..iter_times{
        score += calc_similarity_between_i64vecs(&vec_a[index], &vec_b[index]) * ((vec_a[index].len() + vec_b[index].len()) / 2).pow(2) as f64;
    }
    score
}


// 计算两个Vec<i64> vec之间的相似度分数
// 传入&Vec<&Vec<i64>>，若需要计算字符串之间的相似度，则需要逐字符转换成ASCII码，并生成Vec<i64>数据列作为参数传入
pub fn calc_similarity_between_vvecs(vec_a: &Vec<Vec<i64>>, vec_b: &Vec<Vec<i64>>) -> f64 {
    calc_similarity_score_between_vvecs(vec_a, vec_b) * 2.0 /
        (calc_similarity_score_between_vvecs(vec_a, vec_a) + calc_similarity_score_between_vvecs(vec_b, vec_b))
}


// 将用户名根据数据类型进行分割，如果遇到英文字符则转换成ASCII码，若遇到阿拉伯数字则直接拼接成最大数字
// 例如："lalala1234lala4t" -> [[108, 97, 108, 97, 108, 97], [1234], [108, 97, 108, 97], [4], [116]]
pub fn split_account_name_by_data_type(account_name: &str) -> (Vec<Vec<i64>>, Vec<i64>, Vec<i64>){
    let mut res_item_list = Vec::new();
    let mut res_skeleton_style:Vec<i64> = Vec::new();
    let mut res_skeleton_part_size_list:Vec<i64> = Vec::new();

    for (_key, group) in &account_name.chars().into_iter().group_by(|elt| '0' <= *elt && *elt <= '9') {
        let tmp_vec = group.collect::<Vec<char>>();

        res_item_list.push(Vec::new());
        for c in tmp_vec.iter(){
            res_item_list.last_mut().unwrap().push(*c as i64);
        }

        if '0' <= tmp_vec[0] && tmp_vec[0] <= '9'{
            res_skeleton_style.push('i' as i64);
        }
        else {
            res_skeleton_style.push('s' as i64);
        }
        res_skeleton_part_size_list.push(tmp_vec.len() as i64);
    }

    (res_item_list, res_skeleton_style, res_skeleton_part_size_list)
}




#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        assert_eq!(calc_edit_distance("asdsf", "asdsq"), 1);
        assert_eq!(calc_jaro_winkler_distance("asdsf", "asdsq"), 0.9555555555555556);
        assert_eq!(calc_similarity_between_digits(124, 127), 0.9762813290793214);
        assert_eq!(calc_score_by_analyze_dif_list(&vec![5, 7, 9]), 4.0227272727272725);
        assert_eq!(calc_similarity_score_between_i64vecs(&vec![5, 7, 9], &vec![6, 7, 8]), 44.5000000000001);
        assert_eq!(calc_similarity_between_i64vecs(&vec![5, 7, 9], &vec![6, 7, 8]), 0.5855263157894742);
        assert_eq!(calc_similarity_score_between_vvecs(&vec![vec![5, 7, 9], vec![97, 99]], &vec![vec![6, 7, 8], vec![98, 100]]), 6.436403508772047);
        assert_eq!(calc_similarity_between_vvecs(&vec![vec![5, 7, 9], vec![97, 99]], &vec![vec![6, 7, 8], vec![98, 100]]), 0.4951079622132306);
        println!("{:?}", split_account_name_by_data_type("lalala1234lala4t"));
        println!("{:?}", calc_similarity_between_i64vecs(&vec![117], &vec![116]));
        println!("{}", calc_similarity_between_vvecs(&vec![vec![5, 7, 9], vec![97, 99]], &vec![vec![6, 7, 8], vec![98, 100]]));
        println!("{}", calc_similarity_between_vvecs(&vec![vec![117]], &vec![vec![116]]));
    }
}
