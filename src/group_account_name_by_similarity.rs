use std::cmp::min;
use std::collections::{HashSet, HashMap};
use itertools::Itertools;
use crossbeam::channel as channel;
use lazy_static::lazy_static;
use crate::analyze_account_name_similarity::{CAccountNameSimAnalyse};


// CPU数量
lazy_static! {
    static ref NUM_OF_CPU_CORES: usize = num_cpus::get();
    static ref DEFAULT_THREAD_MAX: usize = *NUM_OF_CPU_CORES + 1;
    static ref DEFAULT_GROUP_GRANULARITY: usize = (*DEFAULT_THREAD_MAX).pow(2) * 400;
    static ref DEFAULT_MASSIVE_DATA_THRETHOLD: usize = (*DEFAULT_THREAD_MAX).pow(2) * 600;
}


// 用于对账号集合进行分组的判定阈值
pub struct CSimilarityGroupingThreshold{
    pub threshold_sim: f64,                 // 相似度阈值,高于等于这个阈值则判定两个账号相似,可以被分为一组
    pub threshold_group_members: usize      // 组员数量阈值,过滤掉成员数量较少的组.  例如threshold_group_members=3, 则返回的结果中仅包含组员数大于3的账号组
}
impl CSimilarityGroupingThreshold{
    pub fn default() -> CSimilarityGroupingThreshold{
        CSimilarityGroupingThreshold{
            threshold_sim: 0.856,
            threshold_group_members: 2
        }
    }
    pub fn set_threshold_sim(&mut self, threshold_sim: f64) -> &mut CSimilarityGroupingThreshold {
        self.threshold_sim = threshold_sim;
        self
    }
    pub fn set_threshold_group_members(&mut self, threshold_group_members: usize) -> &mut CSimilarityGroupingThreshold {
        self.threshold_group_members = threshold_group_members;
        self
    }
}

// 效率档位
#[derive(Clone, Copy)]
pub enum EfficiencyMode {
    Accurately = 0,
    Normal = 1,
    Quickly = 2,
    Rapidly = 3
}
impl EfficiencyMode{
    pub fn default() -> EfficiencyMode{
        EfficiencyMode::Quickly
    }
}

/// # Description
/// * CAccountNameAnaVec会帮助你分析账号名集合，并将相似的账号名进行聚类。
/// * CAccountNameAnaVec will help you analyze the collection of account names and cluster similar account names.
/// # Function
/// * CAccountNameAnaVec提供四种分组模式，它们的精准度和运行效率各有不同，您可以根据场景需求选择合适的模式。它们分别是:
/// * CAccountNameAnaVec provides four grouping modes with different accuracy and operating efficiency. You can choose the appropriate mode according to the needs of the scene. They are:
///
///     1.group_by_similarity_accurately
///
///     2.group_by_similarity
///
///     3.group_by_similarity_quickly
///
///     4.group_by_similarity_rapidly
///
/// * 从命名上就能很轻易地看出，group_by_similarity_rapidly的运行效率最高，group_by_similarity_accurately的精准度最高。
/// * 传入的threshold_sim、threshold_group_members两项参数会很大程度影响运行的效率
pub struct CAccountNameAnaVec<'a>{
    analyse_obj_vec: Vec<CAccountNameSimAnalyse<'a>>,
    data_vec_size: usize,
}
impl<'a> CAccountNameAnaVec<'a>{

    /// 这是这个类的初始化函数。传入账号名集合，初始化流程中会使用特定规则会对其进行初步的去重、排序
    ///
    /// This is the initialization function of this class.
    /// You need to pass in a set of account names as parameters, and specific rules will be used in the initialization process to perform preliminary deduplication and sorting
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

    /// 返回排序后的账号名集合
    ///
    /// Return the sorted account name collection
    pub fn to_vec(&self) -> Vec<String>{
        let mut res: Vec<String> = Vec::new();
        for i in 0..self.data_vec_size{
            res.push(self.analyse_obj_vec[i].account_name.parse().unwrap());
        }
        res
    }

    /// # 功能
    /// 以账号之间的相似度作为判断标准对账号集合进行分组，高度相似的账号会被分到一组。
    ///
    /// # 参数
    ///
    /// # 返回
    ///
    ///
    /// # Function
    /// This function uses the similarity between accounts as the criterion to group account sets, and highly similar accounts will be grouped together.
    ///
    /// 按运行效率排序(Sort by operating efficiency) :
    ///
    /// group_by_similarity_accurately < group_by_similarity << group_by_similarity_quickly < group_by_similarity_rapidly
    ///
    /// 按结果的精准度排序(Sort by accuracy of results) :
    ///
    pub fn group_by_similarity_accurately(&self, threshold_sim: f64, threshold_group_members: usize) -> HashMap<usize, Vec<String>>{
        self.group_account_names_by_similarity(&mut CSimilarityGroupingThreshold { threshold_sim, threshold_group_members }, &EfficiencyMode::Accurately)
    }
    pub fn group_by_similarity(&self, threshold_sim: f64, threshold_group_members: usize) -> HashMap<usize, Vec<String>>{
        self.group_account_names_by_similarity(&mut CSimilarityGroupingThreshold { threshold_sim, threshold_group_members }, &EfficiencyMode::Normal)
    }
    pub fn group_by_similarity_quickly(&self, threshold_sim: f64, threshold_group_members: usize) -> HashMap<usize, Vec<String>>{
        self.group_account_names_by_similarity(&mut CSimilarityGroupingThreshold { threshold_sim, threshold_group_members }, &EfficiencyMode::Quickly)
    }
    pub fn group_by_similarity_rapidly(&self, threshold_sim: f64, threshold_group_members: usize) -> HashMap<usize, Vec<String>>{
        self.group_account_names_by_similarity(&mut CSimilarityGroupingThreshold { threshold_sim, threshold_group_members }, &EfficiencyMode::Rapidly)
    }

    // 基于相似度对账号名称进行分组
    // threshold: 阈值
    // mode: 效率档位,该值越高,效率越高,但结果会遗漏部分数据
    // 根据分割粒度将数据分割成若干数据块，并分别交由子线程处理,最后进行数据汇总
    // 设置合理的分割粒度可以在保证准确性的基础上提高运算效率
    fn group_account_names_by_similarity(&self, threshold: &mut CSimilarityGroupingThreshold, mode: &EfficiencyMode) -> HashMap<usize, Vec<String>>{
        let group_index_map: HashMap<usize, Vec<usize>>;
        if threshold.threshold_sim > 1.0{
            threshold.threshold_sim = 1.0;
        }
        // 数据量较大，需要采用 group_massive_accounts
        if self.data_vec_size >= *DEFAULT_MASSIVE_DATA_THRETHOLD{
            group_index_map = self.group_massive_accounts(&(0..self.data_vec_size).collect_vec(), threshold, *DEFAULT_GROUP_GRANULARITY, mode);
        }else {
            // 处理小数据量的账号
            match mode {
                EfficiencyMode::Accurately | EfficiencyMode::Normal  => {group_index_map = self.group_accurately(&(0..self.data_vec_size).collect_vec(), threshold);},
                EfficiencyMode::Quickly    | EfficiencyMode::Rapidly => {group_index_map = self.group_quickly(&(0..self.data_vec_size).collect_vec(), threshold);},
            };
        }

        self.generate_group_map_by_index(&group_index_map, threshold.threshold_group_members)
    }

    // 对大量数据进行分组
    fn group_massive_accounts(&self, index_vec: &Vec<usize>, threshold: &CSimilarityGroupingThreshold, group_granularity: usize, mode: &EfficiencyMode) -> HashMap<usize, Vec<usize>>{
        let mut b_efficient = false;
        let mut fn_pointer: fn(&CAccountNameAnaVec<'a>, &Vec<usize>, &CSimilarityGroupingThreshold) -> HashMap<usize, Vec<usize>> = CAccountNameAnaVec::group_accurately;
        match mode {
            EfficiencyMode::Accurately => {},
            EfficiencyMode::Normal     => {b_efficient = true;},
            EfficiencyMode::Quickly    => {fn_pointer = CAccountNameAnaVec::group_quickly;},
            EfficiencyMode::Rapidly    => {b_efficient = true; fn_pointer = CAccountNameAnaVec::group_quickly;}
        };
        let account_groups_vec = self.split_index_vec(index_vec, group_granularity);
        self.fn_handler_group(&account_groups_vec, threshold,  b_efficient, &fn_pointer)
    }

    // 对少量数据准确分组
    fn group_accurately(&self, index_list: &Vec<usize>, threshold: &CSimilarityGroupingThreshold) -> HashMap<usize, Vec<usize>>{
        self.basic_worker_group(index_list, threshold, 400, false)
    }

    // 对少量数据快速分组
    fn group_quickly(&self, index_list: &Vec<usize>, threshold: &CSimilarityGroupingThreshold) -> HashMap<usize, Vec<usize>>{
        self.basic_worker_group(index_list, threshold, 400, true)
    }

    // 对数据进行分组
    fn basic_worker_group(&self, index_vec: &Vec<usize>, threshold: &CSimilarityGroupingThreshold, group_granularity: usize, b_efficient: bool) -> HashMap<usize, Vec<usize>>{
        let fn_pointer: fn(&CAccountNameAnaVec<'a>, &Vec<usize>, &CSimilarityGroupingThreshold) -> HashMap<usize, Vec<usize>> = CAccountNameAnaVec::worker_group_accounts_bottommost;
        let account_groups_vec = self.split_index_vec(index_vec, group_granularity);
        self.fn_handler_group(&account_groups_vec,
                              &CSimilarityGroupingThreshold {
                                  threshold_sim: threshold.threshold_sim,
                                  threshold_group_members: min(group_granularity / 100, threshold.threshold_group_members) },
                              b_efficient, &fn_pointer)
    }

    // 传入函数指针,handler内部多线程执行该函数并将结果汇总
    // 分为快速模式和精准模式,如果需要快速计算,可将b_efficient设置为true,这可能会导致少量数据被遗弃,但在计算大量数据的过程中可以显著提高效率
    // b_recursion用于退出合并递归,主动调用fn_handler_group时该值均为true
    fn fn_handler_group(&self, account_groups_vec: &Vec<Vec<usize>>, threshold: &CSimilarityGroupingThreshold, b_efficient: bool, fn_pointer: &fn(&CAccountNameAnaVec<'a>, &Vec<usize>, &CSimilarityGroupingThreshold) -> HashMap<usize, Vec<usize>>) -> HashMap<usize, Vec<usize>>{
        let thread_num = account_groups_vec.len();

        // 单线程可处理
        if thread_num == 1{
            return fn_pointer(&self, &account_groups_vec[0], threshold);
        }

        // 需要用到多线程
        let mut result: HashMap<usize, Vec<usize>> = HashMap::new();
        let (s, r) = channel::bounded(thread_num);
        match thread_num{
            // 不需要使用线程池
            thread_num if thread_num <= *DEFAULT_THREAD_MAX => {
                for i in 0..thread_num{
                    crossbeam::scope(|scope| {
                        scope.spawn(|_|{
                            s.clone().send(fn_pointer(&self, &account_groups_vec[i].to_vec(), threshold)).unwrap();
                        });
                    }).unwrap();
                };
            },

            // 使用线程池，避免线程切换/申请/销毁占用过多资源
            _ => {
                let pool = rayon::ThreadPoolBuilder::new().num_threads(*DEFAULT_THREAD_MAX).build().unwrap();
                for i in 0..thread_num{
                    pool.install(|| s.clone().send(fn_pointer(&self, &account_groups_vec[i].to_vec(), threshold)).unwrap());
                };
            }
        };


        // 整合数据
        for _i in 0..thread_num{
            let mut map_to_integrate = r.recv().unwrap();
            // 优化掉一些低频数据, 效率高，但会造成部分数据的丢失
            if b_efficient{
                self.filter_low_frequency_data(&mut map_to_integrate, threshold.threshold_group_members);
            }

            // 数据合并
            self.integrate_two_group_map(&mut result, &map_to_integrate, threshold.threshold_sim);
        }
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
    fn integrate_two_group_map(&self, dst: &mut HashMap<usize, Vec<usize>>, src: &HashMap<usize, Vec<usize>>, threshold_sim: f64){
        let thread_num = src.len();
        if thread_num == 0{
            return;
        }

        let (s, r) = channel::bounded(thread_num);
        let origin_dst = dst.clone();
        match thread_num {
            thread_num if thread_num <= *DEFAULT_THREAD_MAX =>{
                for key in src.keys(){
                    crossbeam::scope(|scope| {
                        scope.spawn(|_|{
                            s.clone().send((*key, self.determine_which_group_the_account_belongs_to(*key, &origin_dst, threshold_sim))).unwrap();
                        });
                    }).unwrap();
                };
            },
            _ => {
                let pool = rayon::ThreadPoolBuilder::new().num_threads(*DEFAULT_THREAD_MAX).build().unwrap();
                for key in src.keys(){
                    pool.install(|| s.clone().send((*key, self.determine_which_group_the_account_belongs_to(*key, &origin_dst, threshold_sim))).unwrap());
                };
            }
        }

        for _i in 0..thread_num{
            let integrate_data = r.recv().unwrap();
            if integrate_data.0 == integrate_data.1{
                dst.entry(integrate_data.1).or_insert(src[&integrate_data.0].clone());
            }else {
                dst.get_mut(&integrate_data.1).unwrap().append(&mut src.get(&integrate_data.0).unwrap().clone());
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

    // 分割成不同源账号组,用于多线程运算
    fn split_index_vec(&self, index_vec: &Vec<usize>, group_granularity: usize) -> Vec<Vec<usize>>{
        let mut result: Vec<Vec<usize>>= Vec::new();
        let total_size = index_vec.len();
        if total_size == 0{
            return result;
        }

        let mut index_vec_group_by_skeleton: Vec<Vec<usize>> = Vec::new();
        index_vec_group_by_skeleton.push(vec![index_vec[0]]);
        for index in 1..index_vec.len(){
            if &self.analyse_obj_vec[index_vec[index - 1]].skeleton_style == &self.analyse_obj_vec[index_vec[index]].skeleton_style &&
                &self.analyse_obj_vec[index_vec[index - 1]].skeleton_part_size_list == &self.analyse_obj_vec[index_vec[index]].skeleton_part_size_list
            {
                let current_tail_index = index_vec_group_by_skeleton.len() - 1;
                index_vec_group_by_skeleton.get_mut(current_tail_index).unwrap().push(index_vec[index]);
            }
            else {
                index_vec_group_by_skeleton.push(vec![index_vec[index]]);
            }
        }
        index_vec_group_by_skeleton.sort_by(|a, b| b.len().cmp(&a.len()));
        for index in 0..index_vec_group_by_skeleton.len(){
            result.append(&mut self.split_index_vec_by_granularity(&index_vec_group_by_skeleton[index], group_granularity));
        }

        result
    }

    // 根据分组粒度对数据进行分组,返回各组成员的序号
    fn split_index_vec_by_granularity(&self, index_vec: &Vec<usize>, group_granularity: usize) -> Vec<Vec<usize>>{
        let mut result: Vec<Vec<usize>>= Vec::new();
        let data_vec_size = index_vec.len();
        let real_group_granularity = min(group_granularity, data_vec_size);
        // 完整组
        for i in 0..(data_vec_size / real_group_granularity){
            result.push(index_vec[i * real_group_granularity..((i + 1) * real_group_granularity)].to_vec());
        }
        // 若最后一组的数据成员过少(数量少于real_group_granularity / 2)，则合并入前一组
        let res_len = result.len();
        let remaining_data_len = data_vec_size - res_len * real_group_granularity;
        if remaining_data_len > 0 {
            if (remaining_data_len as f64) < (real_group_granularity as f64 / 2.0){
                result[res_len - 1].append(&mut index_vec[res_len * real_group_granularity..data_vec_size].to_vec());
            }
            else {
                result.push(index_vec[res_len * real_group_granularity..data_vec_size].to_vec());
            }
        }
        result
    }

    // 生成账号组信息表
    fn generate_group_map_by_index(&self, index_map: &HashMap<usize, Vec<usize>>, threshold_group_members: usize) -> HashMap<usize, Vec<String>>{
        let mut group_index: usize = 0;
        let mut group_map: HashMap<usize, Vec<String>> = HashMap::new();
        let mut group_vec = index_map.iter().collect_vec();
        group_vec.sort_by(|a, b| b.1.len().cmp(&a.1.len()));
        for group in group_vec{
            // 因为前面排过序，所以当遇到组员数量少于阈值的情况直接结束遍历
            if group.1.len() < threshold_group_members{
                break;
            }

            let mut group_detail: Vec<String> = Vec::new();
            for index in group.1{
                group_detail.push(self.analyse_obj_vec[*index].account_name.parse().unwrap());
            }
            group_map.entry(group_index).or_insert(group_detail);
            group_index += 1;
        }
        group_map
    }

    // 过滤部分低频数据
    fn filter_low_frequency_data(&self, src: &mut HashMap<usize, Vec<usize>>, threshold: usize){
        // 舍弃低频数据
        let mut remove_vec: Vec<usize> = Vec::new();
        for it in src.iter_mut(){
            if it.1.len() < threshold{
                remove_vec.push(*it.0);
            }
        }
        for i in remove_vec{
            src.remove(&i);
        }
    }
}





#[cfg(test)]
mod tests {
    extern crate serde_json;

    use std::fs;
    use super::*;

    #[test]
    fn it_works() {

        // let mut account_vec: Vec<&str> = Vec::new();  // 存储账号名
        // let account_list = fs::read_to_string(".\\test_data\\test_account_list.txt").unwrap();  // 测试25847条数据
        // for account_name in account_list.lines(){
        //     account_vec.push(&account_name);
        // }
        // let ana = CAccountNameAnaVec::new(&account_vec);
        // for item in ana.to_vec(){
        //     println!("{}", &item);
        // }
        // let group_res = ana.group_account_names_by_similarity(&CSimilarityGroupingThreshold::default().set_threshold_sim(0.82).set_threshold_group_members(5), &EfficiencyMode::Quickly);
        // println!("{}", serde_json::to_string_pretty(&serde_json::json!(&group_res)).unwrap());
        // fs::write(".\\test_data\\result\\result__5__0_85.txt", serde_json::to_string_pretty(&serde_json::json!(&group_res)).unwrap());

        // let mut account_vec: Vec<&str> = Vec::new();  // 存储账号名
        // let account_list = fs::read_to_string(".\\test_data\\test_massive_account_list.txt").unwrap();  // 大量数据测试，866411个去重账号
        // for account_name in account_list.lines(){
        //     account_vec.push(&account_name);
        // }
        // let ana = CAccountNameAnaVec::new(&account_vec);
        // let group_res = ana.group_account_names_by_similarity(&CSimilarityGroupingThreshold::default().set_threshold_sim(0.82).set_threshold_group_members(10), &EfficiencyMode::Rapidly);
        // // println!("{}", serde_json::to_string_pretty(&serde_json::json!(&group_res)).unwrap());
        // fs::write(".\\test_data\\result\\result_massive__10__0_82.txt", serde_json::to_string_pretty(&serde_json::json!(&group_res)).unwrap());


        // let vec_obj = vec!["a1f6", "aa11ff66", "b2c", "a1f55", "1"];
        // let tmp = CAccountNameAnaVec::new(&vec_obj);
        // let _res= tmp.group_account_names_by_similarity(&CSimilarityGroupingThreshold { threshold_sim: 0.856, threshold_group_members: 1 }, &EfficiencyMode::Accurately);
        // for item in _res.iter(){
        //     println!("{}-{:?}", item.0, item.1)
        // }
    }
}