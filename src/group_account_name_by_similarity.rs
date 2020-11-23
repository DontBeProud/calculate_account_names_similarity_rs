use std::cmp::min;
use std::collections::{HashSet, HashMap};
use itertools::Itertools;
use crossbeam::channel as channel;
use lazy_static::lazy_static;
use crate::analyze_account_name_similarity::{CAccountNameSimAnalyse};


// CPU数量
lazy_static! {
    static ref NUM_OF_CPU_CORES: usize = num_cpus::get();
    static ref DEFAULT_THREAD_MAX: usize = num_cpus::get() + 1;
}


// 用于对账号集合进行分组的判定阈值
pub struct CSimilarityGroupingThreshold{
    pub threshold_sim: f64,                 // 相似度阈值,高于等于这个阈值则判定两个账号相似,可以被分为一组
    pub threshold_frequency: f64,           // 频率阈值,即某组的成员占所在账号块总帐号(划分后的某一账号块,非所有账号)的比例,数值高低需要根据应用场景决定,过低会影响效率
    pub threshold_integrate: f64,           // 整合阈值,用于将多线程运算的结果整合,此值过高可能导致本应属于一个账号组的账号被划分至多组,过低会导致本应划分为多组的账号被划分入一组
    pub threshold_group_members: f64        // 组员数量阈值,过滤掉成员数量较少的组
}
impl CSimilarityGroupingThreshold{
    pub fn default() -> CSimilarityGroupingThreshold{
        CSimilarityGroupingThreshold{
            threshold_sim: 0.856,
            threshold_frequency: 0.0002,
            threshold_integrate: 0.856,
            threshold_group_members: 1.0
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
pub enum EfficiencyMode {
    Accurately = 0,
    Normal = 1,
    Quickly = 2,
    Rapidly = 3
}
impl EfficiencyMode{
    pub fn default() -> EfficiencyMode{
        EfficiencyMode::Accurately
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
    pub fn group_account_names_by_similarity(&self, threshold: &CSimilarityGroupingThreshold, group_granularity: usize, mode: &EfficiencyMode) -> HashMap<usize, Vec<usize>>{
        self.group_massive_accounts(threshold, group_granularity, mode)
    }

    // 对大量数据进行分组
    fn group_massive_accounts(&self, threshold: &CSimilarityGroupingThreshold, group_granularity: usize, mode: &EfficiencyMode) -> HashMap<usize, Vec<usize>>{
        let index_vec = &(0..self.data_vec_size).collect_vec();
        let mut b_efficient = false;
        let mut fn_pointer: fn(&CAccountNameAnaVec<'a>, &Vec<usize>, &CSimilarityGroupingThreshold) -> HashMap<usize, Vec<usize>> = CAccountNameAnaVec::group_accurately;
        match mode {
            EfficiencyMode::Accurately => {},
            EfficiencyMode::Normal     => {b_efficient = true;},
            EfficiencyMode::Quickly    => {fn_pointer = CAccountNameAnaVec::group_quickly;},
            EfficiencyMode::Rapidly    => {b_efficient = true; fn_pointer = CAccountNameAnaVec::group_quickly;}
        };

        self.fn_handler_group(index_vec, threshold, group_granularity, b_efficient, true, &fn_pointer)
    }

    // 对少量数据准确分组
    fn group_accurately(&self, index_list: &Vec<usize>, threshold: &CSimilarityGroupingThreshold) -> HashMap<usize, Vec<usize>>{
        self.worker_group(index_list, threshold, 400, false, true)
    }

    // 对少量数2020/11/23 9:54:14 已读据快速分组
    fn group_quickly(&self, index_list: &Vec<usize>, threshold: &CSimilarityGroupingThreshold) -> HashMap<usize, Vec<usize>>{
        self.worker_group(index_list, threshold, 400, true, true)
    }

    // 对数据进行分组
    fn worker_group(&self, index_vec: &Vec<usize>, threshold: &CSimilarityGroupingThreshold, group_granularity: usize, b_efficient: bool, b_recursion: bool) -> HashMap<usize, Vec<usize>>{
        let fn_pointer: fn(&CAccountNameAnaVec<'a>, &Vec<usize>, &CSimilarityGroupingThreshold) -> HashMap<usize, Vec<usize>> = CAccountNameAnaVec::worker_group_accounts_bottommost;
        self.fn_handler_group(index_vec, threshold, group_granularity, b_efficient, b_recursion, &fn_pointer)
    }

    // 传入函数指针,handler内部多线程执行该函数并将结果汇总
    // 分为快速模式和精准模式,如果需要快速计算,可将b_efficient设置为true,这可能会导致少量数据被遗弃,但在计算大量数据的过程中可以显著提高效率
    // b_recursion用于退出合并递归,主动调用fn_handler_group时该值均为true
    fn fn_handler_group(&self, index_vec: &Vec<usize>, threshold: &CSimilarityGroupingThreshold, group_granularity: usize, b_efficient: bool, b_recursion: bool, fn_pointer: &fn(&CAccountNameAnaVec<'a>, &Vec<usize>, &CSimilarityGroupingThreshold) -> HashMap<usize, Vec<usize>>) -> HashMap<usize, Vec<usize>>{
        let master_accounts_groups_vec = self.split_index_vec_by_granularity(index_vec, group_granularity);
        let thread_num = master_accounts_groups_vec.len();

        // 单线程可处理
        if thread_num == 1{
            return fn_pointer(&self, &master_accounts_groups_vec[0], threshold);
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
                            s.clone().send(fn_pointer(&self, &master_accounts_groups_vec[i].to_vec(), threshold)).unwrap();
                        });
                    }).unwrap();
                };
            },

            // 使用线程池，避免线程切换/申请/销毁占用过多资源
            _ => {
                let pool = rayon::ThreadPoolBuilder::new().num_threads(*DEFAULT_THREAD_MAX).build().unwrap();
                for i in 0..thread_num{
                    pool.install(|| s.clone().send(fn_pointer(&self, &master_accounts_groups_vec[i].to_vec(), threshold)).unwrap());
                };
            }
        };


        for _i in 0..thread_num{
            let mut map_to_integrate = r.recv().unwrap();
            if b_efficient{
                // Fast, 效率高，但会造成部分数据的丢失
                self.filter_low_frequency_data(&mut map_to_integrate, threshold.threshold_frequency);
            }

            // 数据合并
            self.integrate_two_group_map(&mut result, &map_to_integrate, threshold.threshold_integrate, b_recursion);
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
    fn integrate_two_group_map(&self, dst: &mut HashMap<usize, Vec<usize>>, src: &HashMap<usize, Vec<usize>>, threshold_integrate: f64, b_recursion: bool){
        if b_recursion && (dst.len() + src.len()) > 600{
            // 合并
            for (index, value) in src.iter(){
                dst.entry(*index).or_insert(value.clone());
            }
            // 对组长进行分组
            let leader_vec = dst.keys().into_iter().map(|&x| x).collect_vec();
            let leader_group = self.worker_group(&leader_vec, &CSimilarityGroupingThreshold::default(), 400, false, false);

            // 根据组长的相似度分组情况，将部分组合并。(若两组的组长相似，则两组合并)
            for (leader, member_list) in leader_group.iter(){
                for member_index in member_list.iter(){
                    if *member_index != *leader{
                        let mut tmp_member_list = dst.get_mut(member_index).unwrap().clone();
                        dst.get_mut(leader).unwrap().append(&mut tmp_member_list);
                        dst.remove(member_index);
                    }
                }
            }
        }else {
            self.integrate_two_group_map_normal(dst, src, threshold_integrate);
        }
    }

    // 将两个存储账号组信息的map融合,单线程匹配
    fn integrate_two_group_map_normal(&self, dst: &mut HashMap<usize, Vec<usize>>, src: &HashMap<usize, Vec<usize>>, threshold_integrate: f64){
        for (src_leader, src_group) in src.iter(){
            let group_leader = self.determine_which_group_the_account_belongs_to(*src_leader, dst, threshold_integrate);
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
        // 完整组
        for i in 0..(data_vec_size / real_group_granularity){
            result.push(index_vec[i * real_group_granularity..((i + 1) * real_group_granularity)].to_vec());
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

    // 过滤部分低频数据
    fn filter_low_frequency_data(&self, src: &mut HashMap<usize, Vec<usize>>, threshold_standard: f64){
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





#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        let vec_obj = vec!["a1f6", "aa11ff66", "b2c", "a1f55", "1"];
        let tmp = CAccountNameAnaVec::new(&vec_obj);
        // println!("{}", num_cpus::get());
        let _res= tmp.group_account_names_by_similarity(&CSimilarityGroupingThreshold::default(), 30000, &EfficiencyMode::Accurately);
        for i in _res.keys().into_iter().map(|&x| x).collect_vec(){
            println!("{}-{:?}", i, _res[&i])
        }
        // println!("{}", _res.keys().into_iter().map(|&x| x).collect_vec().len());

    }
}