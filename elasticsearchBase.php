<?php


/**
 * elasticsearch封装类
 */

require ROOT_DIR.'/composer/vendor/autoload.php';
use Elasticsearch\ClientBuilder;


class elasticsearchBase
{

    public $config;
    public $api;
    public $index_name;
    public $index_type;

    public function __construct($index_name = false, $index_type = false)
    {
        try {
            if (!defined('ES_START')) {
                throw new Exception("未配置ES信息");
            }
            $esHost = defined('ES_HOST') ? ES_HOST : '127.0.0.1:9200';

            //加载配置文件
            $hosts = [
                $esHost,   // IP + Port
            ];
            //构建客户端对象
            $this->api = ClientBuilder::create()->setHosts($hosts)->build();
            if($this->api->ping() != 'true') {
                // throw new Exception("ES服务未启动");
                return true;
            }
            //默认索引类型未设置，索引类型等于索引名称
            if(($index_name !== false) && ($index_type ===false)) {
                $index_type = $index_name;
            }
            if($index_name !== false) {
                $this->index_name = $index_name;
            }
            if($index_type !== false) {
                $this->index_type = $index_type;
            }
        } catch (\Exception $e) {
            throw $e;
        }
    }

    /**
     * 创建一个索引
     * @param $settings
     * @return array
     * @throws \Exception
     * @author cp
     */
    public function createIndex($settings = [])
    {
        try {
            $initParams['index'] = $this->index_name;
            !empty($settings) && $initParams['body']['settings'] = $settings;

            $res = $this->api->indices()->create($initParams);

        } catch (\Exception $e) {
            throw $e;
        }

        return $res;
    }

    /**
     * 查询索引是否存在
     * @return array|bool
     * @throws \Exception
     */
    public function exist()
    {
        try {
            $params['index'] = $this->index_name;

            $res = $this->api->indices()->exists($params);

        } catch (\Exception $e) {
            throw $e;
        }
        return $res;
    }

    /**
     * 更新索引的映射 mapping
     * @param $data
     *  核心数据类型
        ● 字符串：text, keyword
        ● 数值型：long, integer, short, byte, double, float, half_float, scaled_float
        ● 布尔型：boolean
        ● 日期型：date, date_nanos
        ● 二进制：binary
        ● 范围型：integer_range, float_range, long_range, double_range, date_range
     * @return array
     * @throws \Exception
     * @author cp
     */
    public function setMapping($data)
    {
        try {
            $initParams = $this->initParams();
            $initParams['body'][$this->index_type]['properties'] = $data;
            $res = $this->api->indices()->putMapping($initParams);
        } catch (\Exception $e) {
            throw $e;
        }
        return $res;
    }

    /**
     * 删除索引
     * @return array|bool
     * @throws \Exception
     */
    public function deleteIndex()
    {
        try {
            $params['index'] = $this->index_name;

            $res = $this->api->indices()->delete($params);

        } catch (\Exception $e) {
            throw $e;
        }
        return $res;
    }

    /**
     * 获取索引映射 mapping
     * @return array
     * @throws \Exception
     * @author cp
     */
    public function getMapping()
    {
        try {
            $initParams = $this->initParams();
            $res = $this->api->indices()->getMapping($initParams);
        } catch (\Exception $e) {
            throw $e;
        }

        return $res;
    }

    /**
     * 新增单条数据
     * @param $data
     * @return bool
     * @throws \Exception
     * @author cp
     */
    public function insert($data)
    {
        try {
            $params = $this->initParams();
            isset($data['id']) && $params['id'] = $data['id'];
            $params['body'] = $data['body'];

            $res = $this->api->index($params);
        } catch (\Exception $e) {
            throw $e;
        }
        if (!isset($res['_shards']['successful']) || !$res['_shards']['successful']) {
            return false;
        }
        return true;
    }

    /**
     * 批量操作数据
     * create	当文档不存在时创建之。
     * index	创建新文档或替换已有文档。
     * update	局部更新文档。
     * delete	删除一个文档。
     * @param $data
     * @return array
     * @throws \Exception
     * @author cp
     */
    public function bulk($data)
    {
        try {
            if (empty($data['body'])) return false;
            $params = $this->initParams();
            $params['body'] = $data['body'];

            $res = $this->api->bulk($params);

        } catch (\Exception $e) {
            throw $e;
        }
        return $res;
    }
    
    /**
     * 检测数据是否存在
     * @param $id
     * @return array|bool
     * @throws \Exception
     */
    public function IndexExists($id)
    {
        try {
            $params = $this->initParams();
            $params['id'] = $id;

            $res = $this->api->exists($params);

        } catch (\Exception $e) {
            throw $e;
        }
        return $res;
    }


    /**
     * 根据唯一id查询数据
     * @param $id
     * @return array
     * @throws \Exception
     * @author cp
     */
    public function searchById($id)
    {
        try {
            $params = $this->initParams();
            $params['id'] = $id;

            $res = $this->api->get($params);
        } catch (\Exception $e) {
            throw $e;
        }
        return $res;
    }

    /**
     * 批量查询，只能根据id来查
     * @param $data
     * @return array
     * @throws \Exception
     * @author:cp
     * @date:2021/1/15
     */
    public function mGet($data)
    {
        try {
            if (!is_array($data)) return [];
            //初始化索引
            $params = $this->initParams();

            if (array_key_exists('fields', $data)) {
                $query['ids'] = $data['fields'];
                $params['body'] = $query;
            }
            $res = $this->api->mget($params);
            return $res;

        } catch (\Exception $e) {
            throw $e;
        }
    }

    /**
     * 根据关键字查询数据
     * 多个字段查询：multi_match
     * @param $data
     * $filter 条件组合
     * $offset 从第几条开始
     * $limit 每页显示数量
     * $orderType 自定义排序字段
     * @return array|bool
     * @throws \Exception
     * @author cp
     */
    public function getList($cols = '*', $filter = array(), $offset = 0, $limit = -1, $orderType = null,$highlight = true)
    {
        try {

            if (!is_array($filter)) {
                return [];
            }
            $params = $this->initParams();
            if ($cols != '*') {
                $params['_source'] = $cols;
            }

            //分页
            if (isset($limit)) {
                $params['size'] = !empty($limit) ? $limit : 1;
                $params['from'] = $offset ? $offset : 0;
            }

            //排序
            if (!empty($orderType)) {
                $sort_list = [];
                if(is_array($orderType)) {
                    $orderType = array_chunk($orderType,2);
                    if(is_array($orderType)) {
                        foreach($orderType as $otv) {
                            $order = strtolower(trim($otv[1]));
                            if(($order != 'desc') && ($order != 'asc')) {
                                continue;
                            }
                            $sort_list[] = [
                                '' . trim($otv[0]) . '' => [
                                    'order' => '' . $order . '',
                                ]
                            ];
                        }
                    }
                }else{
                    if($order_type = explode(',',$orderType)) {
                        foreach ($order_type as $otv) {
                            $otv = explode(" ",$otv);
                            $order = strtolower(trim($otv[1]));
                            if(($order != 'desc') && ($order != 'asc')) {
                                continue;
                            }
                            $sort_list[] = [
                                '' . trim($otv[0]) . '' => [
                                    'order' => '' . $order . '',
                                ]
                            ];
                        }
                    }
                }
                if($sort_list) {
                    $params['body']['sort'] = $sort_list;
                }
            }

            /**
             * 深度（滚动）分页
             */
            /*if (array_key_exists('scroll', $filter)) {
                $params['scroll'] = $filter['scroll'];
            }*/

            //条件组合
            if($filter) {
                if($filter = $this->_filter($filter)) {
                    $params['body'] = $filter;
                }
            }
            if($highlight) {
                $params['body']['highlight']['fields']['*'] = new \stdClass();
                $params['body']['highlight']['pre_tags'] = ["<strong style=\"color:red\">"];
                $params['body']['highlight']['post_tags'] = ["</strong>"];
                $params['body']['highlight']['require_field_match'] = true;
            }
            $res = $this->api->search($params);
        } catch (\Exception $e) {
            throw $e;
        }
        return $res['hits']['hits'];
    }

    /**
     * 统计条数
     * $filter 条件组合
    */
    public function count($filter = array()) {
        $params = $this->initParams();
        if($filter) {
            if($filter = $this->_filter($filter)) {
                $params['body'] = $filter;
            }
        }
        return $this->api->count($params);
    }

    /**
     * 根据唯一id删除
     * @param $id
     * @return bool
     * @throws \Exception
     * @author cp
     */
    public function delete($id)
    {
        try {
            $params = $this->initParams();
            $params['id'] = $id;

            $res = $this->api->delete($params);
        } catch (\Exception $e) {
            throw $e;
        }
        if (!isset($res['_shards']['successful'])) {
            return false;
        }
        return true;
    }

    /**
     * 删除所有数据
     * @return bool
     * @throws \Exception
     * @author cp
     */
    public function deleteAll() {
        try {
            $params = $this->initParams();
            $params['body'] = [];
            $res = $this->api->deleteByQuery($params);
        } catch (\Exception $e) {
            throw $e;
        }
        if (!isset($res['_shards']['successful'])) {
            return false;
        }
        return true;
    }

    /**
     * 聚合统计,方差
     * @param $data
     * @return array
     * @throws \Exception
     * @author:cp
     * @date:2021/1/15
     */
    public function searchAgs($data)
    {
        try {
            if (!is_array($data)) {
                return [];
            }
            $query = [];
            $params = $this->initParams();
            $params['size'] = 0;

            /**
             * 条件组合过滤，筛选条件
             */
            if (array_key_exists('condition', $data)) {
                $condition = $data['condition'];
                if (array_key_exists('bool', $condition)) {
                    //必须满足
                    if (array_key_exists('must', $condition['bool'])) {
                        foreach ($condition['bool']['must'] as $key => $val) {
                            if (is_array($val)) {
                                $query['bool']['must'][]['range'] = [
                                    $key => [
                                        'gte' => $val[0],
                                        'lte' => $val[1]
                                    ]
                                ];
                            } else {
                                $query['bool']['must'][]['match'] = [
                                    $key => $val
                                ];
                            }
                        }
                        $params['body']['query'] = $query;
                    }
                }
            }

            //分组、排序设置
            if (array_key_exists('agg', $data)) {
                $agg = [];
                //字段值
                if (array_key_exists('terms', $data['agg'])) {
                    $agg['_result']['terms'] = [
                        'field' => $data['agg']['terms'],
                        'size' => 500,
                    ];
                    if (array_key_exists('order', $data['agg'])) {
                        foreach ($data['agg']['order'] as $key => $val) {
                            $fields = 'result.' . $key;
                            $agg['_result']['terms']['order'] = [
                                $fields => $val
                            ];
                            unset($fields);
                        }
                    }
                }
                //统计
                if (array_key_exists('field', $data['agg'])) {
                    $agg['_result']['aggs'] = [
                        'result' => [
                            'extended_stats' => [
                                'field' => $data['agg']['field']
                            ]
                        ]
                    ];
                }

                //日期聚合统计
                if (array_key_exists('date', $data['agg'])) {
                    $date_agg = $data['agg']['date'];
                    //根据日期分组
                    if (array_key_exists('field', $date_agg)) {
                        $agg['result'] = [
                            'date_histogram' => [
                                'field' => $data['agg']['date']['field'],
                                'interval' => '2h',
                                'taskat' => 'yyyy-MM-dd  HH:mm:ss'
                            ]
                        ];
                    }

                    if (array_key_exists('agg', $date_agg)) {
                        //分组

                        if (array_key_exists('terms', $date_agg['agg'])) {
                            $agg['result']['aggs']['result']['terms'] = [
                                'field' => $date_agg['agg']['terms'],
                                'size' => 100,
                            ];
                        }
                        //统计最大、最小值等
                        if (array_key_exists('stats', $date_agg['agg'])) {
                            $agg['result']['aggs']['result']['aggs'] = [
                                'result_stats' => [
                                    'extended_stats' => [
                                        'field' => $date_agg['agg']['stats']
                                    ]
                                ]
                            ];
                        }
                    }

                }
                $params['body']['aggs'] = $agg;
            }
            \Log::info(json_encode($params));
            $res = $this->api->search($params);

        } catch (\Exception $e) {
            throw $e;
        }
        return $res;
    }

    /**
     * 初始化索引参数
     * @return array
     * @author cp
     */
    public function initParams()
    {
        return [
            'index' => $this->index_name,
            'type' => $this->index_type,
        ];
    }

    /**
     * 条件转换ES语法
     * @param
     * $filter 条件组合
     */
    public function _filter($filter) {
        $res = [];
        foreach ($filter as $k => $v) {
            $col = trim($k);
            $type = '';
            $colArr = strpos($col, '|') ? explode('|', $col) : false;
            if ($colArr) {
                $col = trim($colArr[0]);
                $type = $colArr[1];
            }
            $this->filter_parser($col, $type, $v, $res);
        }

        if (!$res) return array();

        foreach ($res as $k => $v) {
            if (!$v) {
                unset($res[$v]);
                continue;
            }
        }

        return [
            'query' => $res
        ];
    }

    /**
     *  @param
     *  $col 字段
     *  $type 类型
     *  $filter 条件
     *  $res 结果
     */
    private function filter_parser($col, $type, $filter, &$res = array()) {
        if ((!(is_array($filter) || is_object($filter))) && !strlen($filter)) {
            return false;
        }
        switch ($type) {
            case 'than': // >
                $res['bool']['filter']['range'][$col]['gt'] =  $filter;
                break;

            case 'bthan': // >=
                $res['bool']['filter']['range'][$col]['gte'] = $filter;
                break;

            case 'lthan': // <
                $res['bool']['filter']['range'][$col]['lt'] = $filter;
                break;

            case 'sthan': // <=
                $res['bool']['filter']['range'][$col]['lte'] = $filter;
                break;

            case 'noequal': // <> !=
                $res['bool']['must_not']['term'][$col] = $filter;

                break;

            case 'between': // between
                $res['bool']['filter']['range'][$col] = array(
                    'gte' => $filter[0],
                    'lte' => $filter[1],
                );
                break;

            case 'in': // in
                if (is_array($filter)) {
                    foreach ($filter as $v) {
                        $res['terms'][$col][] = $v;
                    }
                } else {
                    $res['terms'][$col][] = $filter;
                }
                break;

            case 'notin': // not in
                if (is_array($filter)) {
                    foreach ($filter as $v) {
                        $res['bool']['must_not'][]['term'] = [
                            $col => $v
                        ];
                    }
                } else {
                    $res['bool']['must_not']['term'][$col] = $filter;
                }
                break;

            case 'has': // has
            case 'head': // head
            case 'foot': // foot
                if (is_array($filter)) {
                    foreach ($filter as $v) {
                        $res['bool']['should'][]['match'] = [
                            $col => $v
                        ];
                    }
                } else {
                    $res['match'][$col] = $filter;
                }
                break;
            case 'nohas': // nohas
                if (is_array($filter)) {
                    foreach ($filter as $v) {
                        $res['bool']['must_not'][]['match'] = [
                            $col => $v
                        ];
                    }
                } else {
                    $res['bool']['must_not']['match'][$col] = $filter;
                }
                break;
            default:
                if (is_array($filter)) {
                    foreach ($filter as $v) {
                        $res['bool']['should'][]['match'] = [
                            $col => $v
                        ];
                    }
                } else {
                    $res['bool']['should'][]['match'][$col]['query'] = $filter;
                    //匹配度
                    //$res['bool']['should'][]['match'][$col]['minimum_should_match'] = "10%";
                }
                break;
        }
        return $filter;
    }

}