use std::cmp::min;
use std::collections::{HashSet, HashMap};
use itertools::Itertools;
use crossbeam::channel as channel;
use crate::analyze_account_name_similarity::{CAccountNameSimAnalyse};


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
            threshold_frequency: 0.0005,
            threshold_integrate: 0.856,
            threshold_group_members: 5.0
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

// 效率档位
#[derive(Clone, Copy)]
pub enum EfficiencyGear {
    Normal = 0,
    Fast = 1,
    VeryFast = 2
}
impl EfficiencyGear{
    pub fn default() -> EfficiencyGear{
        EfficiencyGear::Normal
    }
}

// 用于对账号集合排序/分组
pub struct CAccountNameAnaVec<'a>{
    analyse_obj_vec: Vec<CAccountNameSimAnalyse<'a>>,
    data_vec_size: usize,
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
    // efficiency_gear: 效率档位,该值越高,效率越高,但结果会遗漏部分数据
    // 根据分割粒度将数据分割成若干数据块，并分别交由子线程处理,最后进行数据汇总
    // 设置合理的分割粒度可以在保证准确性的基础上提高运算效率
    pub fn group_account_names_by_similarity(&self, threshold: &CSimilarityGroupingThreshold, group_granularity: usize, efficiency_gear: &EfficiencyGear) -> HashMap<usize, Vec<usize>>{
        let mut result: HashMap<usize, Vec<usize>> = HashMap::new();
        let master_accounts_groups_vec = self.split_analyse_obj_vec_by_granularity(group_granularity);
        let thread_num = master_accounts_groups_vec.len();
        let (s, r) = channel::bounded(thread_num);
        for i in 0..thread_num{
            let sc= s.clone();
            crossbeam::scope(|scope| {
                scope.spawn(|_|{
                    sc.send(self.worker_group_and_analyze( &master_accounts_groups_vec[i], threshold, 400, efficiency_gear)).unwrap();
                });
            }).unwrap();
        };

        for _i in 0..thread_num{
            let mut map_to_integrate = r.recv().unwrap();
            self.filter_low_frequency_data(&mut map_to_integrate, efficiency_gear, EfficiencyGear::VeryFast, threshold.threshold_frequency);
            self.integrate_two_group_map(&mut result, &map_to_integrate, threshold.threshold_integrate);
        }
        result
    }

    fn worker_group_and_analyze(&self, index_vec: &Vec<usize>, threshold: &CSimilarityGroupingThreshold, group_granularity: usize, efficiency_gear: &EfficiencyGear) -> HashMap<usize, Vec<usize>>{
        let mut result: HashMap<usize, Vec<usize>> = HashMap::new();
        let master_accounts_groups_vec = self.split_index_vec_by_granularity(index_vec, group_granularity);
        let thread_num = master_accounts_groups_vec.len();
        let (s, r) = channel::bounded(thread_num);
        for i in 0..thread_num{
            let sc= s.clone();
            crossbeam::scope(|scope| {
                scope.spawn(|_|{
                    sc.send(self.worker_group_accounts_bottommost(&master_accounts_groups_vec[i], threshold)).unwrap();
                });
            }).unwrap();
        };

        for _i in 0..thread_num{
            let mut map_to_integrate = r.recv().unwrap();
            // Very Fast
            self.filter_low_frequency_data(&mut map_to_integrate, efficiency_gear, EfficiencyGear::VeryFast, threshold.threshold_frequency / 2.0);
            self.integrate_two_group_map(&mut result, &map_to_integrate, threshold.threshold_integrate);
        }

        // Fast
        self.filter_low_frequency_data(&mut result, efficiency_gear, EfficiencyGear::Fast, threshold.threshold_frequency / 2.0);
        result
    }

    // 最底层的工作者线程
    fn worker_group_accounts_bottommost(&self, index_list: &Vec<usize>, threshold: &CSimilarityGroupingThreshold) -> HashMap<usize, Vec<usize>>{
        let mut group_map: HashMap<usize, Vec<usize>> = HashMap::new();
        for index in index_list{
            self.integrate_account_into_groups(*index, &mut group_map, threshold.threshold_sim);
        }
        group_map
    }

    // 将两个存储账号组信息的map融合
    fn integrate_two_group_map(&self, dst: &mut HashMap<usize, Vec<usize>>, src: &HashMap<usize, Vec<usize>>, threshold: f64){
        for (src_leader, src_group) in src.iter(){
            let group_leader = self.determine_which_group_the_account_belongs_to(*src_leader, dst, threshold);
            if group_leader == *src_leader{
                dst.entry(group_leader).or_insert(src_group.clone());
            }
            else {
                dst.get_mut(&group_leader).unwrap().append(&mut src_group.clone());
            }
        }
    }

    // 将单个账号与当前账号组匹配，若与某个组的组长相似则划入该组，否则新立一个组
    fn integrate_account_into_groups(&self, index_to_match: usize, group_map: &mut HashMap<usize, Vec<usize>>, threshold: f64){
        let group_leader = self.determine_which_group_the_account_belongs_to(index_to_match, group_map, threshold);
        if group_leader == index_to_match{
            group_map.entry(index_to_match).or_insert(vec![index_to_match]);
        }
        else {
            group_map.get_mut(&group_leader).unwrap().push(index_to_match);
        }
    }

    // 判断某账号所属的组  返回组长的序号  若返回的序号为该账号自身的序号，则说明它不属于任何一组
    fn determine_which_group_the_account_belongs_to(&self, index_to_match: usize, group_map: &HashMap<usize, Vec<usize>>, threshold: f64) -> usize{
        let mut index_vec_to_iter = group_map.keys().collect_vec();
        index_vec_to_iter.sort_by_cached_key(|k| ((**k as i64 - index_to_match as i64).abs()));
        for group_leader_index in index_vec_to_iter{
            if self.analyse_obj_vec[*group_leader_index].calc_similarity(&self.analyse_obj_vec[index_to_match]).0 >= threshold{
                return *group_leader_index;
            }
        }
        index_to_match
    }

    // 对analyse_obj_vec进行初步分组
    fn split_analyse_obj_vec_by_granularity(&self, group_granularity: usize) -> Vec<Vec<usize>>{
        self.split_index_vec_by_granularity(&(0..self.data_vec_size).collect_vec(), group_granularity)
    }

    // 根据分组粒度对数据进行分组,返回各组成员的序号
    fn split_index_vec_by_granularity(&self, index_vec: &Vec<usize>, group_granularity: usize) -> Vec<Vec<usize>>{
        let mut result: Vec<Vec<usize>>= Vec::new();
        let data_vec_size = index_vec.len();
        let real_group_granularity = min(group_granularity, data_vec_size);
        for i in 0..(data_vec_size / real_group_granularity){
            result.push((i * real_group_granularity..((i + 1) * real_group_granularity)).collect_vec());
        }
        // 若最后一组的数据成员过少(数量少于real_group_granularity / 2)，则合并入前一组
        let res_len = result.len();
        let remaining_data_len = data_vec_size - res_len * real_group_granularity;
        if remaining_data_len > 0 {
            if (remaining_data_len as f64) < (real_group_granularity as f64 / 2.0){
                result[res_len - 1].append(&mut (res_len * real_group_granularity..data_vec_size).collect_vec());
            }
            else {
                result.push((res_len * real_group_granularity..data_vec_size).collect_vec());
            }
        }
        result
    }

    // 根据效率档位过滤部分低频数据
    fn filter_low_frequency_data(&self, src: &mut HashMap<usize, Vec<usize>>, efficiency_gear: &EfficiencyGear, current_context_gear: EfficiencyGear, threshold_standard: f64){
        if *efficiency_gear as usize >= current_context_gear as usize {
            // 计算总共多少数据成员
            let mut account_total = 0;
            for value in src.values(){
                account_total += value.len();
            }
            // 舍弃低频数据
            let mut remove_vec: Vec<usize> = Vec::new();
            for it in src.iter_mut(){
                if (it.1.len() as f64) < threshold_standard * account_total as f64{
                    remove_vec.push(*it.0);
                }
            }
            for i in remove_vec{
                src.remove(&i);
            }
        }
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
        // for i in tmp.split_analyse_obj_vec_by_granularity(4){
        //     println!("{:?}", i);
        // }
        println!("{}", num_cpus::get());
        let _res= tmp.group_account_names_by_similarity(&CSimilarityGroupingThreshold::default(), 3000, &EfficiencyGear::default());
        // println!("{}", res.);

    }
}