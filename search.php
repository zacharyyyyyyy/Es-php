<?php
namespace EsPhp;
require 'vendor/autoload.php';
use Elasticsearch\ClientBuilder;
class EsSearch {
    private $client;
    private $param = [];
    public function __construct($host = ['http://127.0.0.1:9200']) {
        try {
            $this->client = ClientBuilder::create()->setHosts($host)->setRetries(3)->build();
        } catch (\Exception $e) {
            return 'Collect fail,error:' . $e->getMessage();
        }
    }

    public static function connection($host=['http://127.0.0.1:9200']){
        $client=new self($host);
        return $client;
    }


    /**
     * 设置搜索索引类型
     * @param $index
     * @param null $type
     * @return $this
     */
    public function base($index, $type = null, $timeout = 20, $connect_timeout = 20) {
        $this->param = [
            'index' => $index,
            'type' => $type,
            'client' => [
                'timeout' => $timeout,
                'connect_timeout' => $connect_timeout
            ]
        ];
        return $this;
    }
    /**
     * 条件判断 并
     * @param $key
     * @param $value
     * @return $this
     */
    public function where($key, $compare, $value, $separate = true) {
        if ($key == 'id') {
            $this->param['id'] = $value;
        } else {
            switch ($compare) {
                case '=':
                    if ($separate == true) {
                        $this->param['body']['query']['bool']['must'][] = [
                            'match' => [
                                $key => $value
                            ]
                        ];
                    } else {
                        $this->param['body']['query']['bool']['must'] = [
                            'term' => [
                                $key => $value
                            ]
                        ];
                    }
                    break;
                case '>':
                    $this->param['body']['query']['bool']['must'][]['range'][$key]['gt'] = $value;
                    break;
                case '<':
                    $this->param['body']['query']['bool']['must'][]['range'][$key]['lt'] = $value;
                    break;
                case '>=':
                    $this->param['body']['query']['bool']['must'][]['range'][$key]['gte'] = $value;
                    break;
                case '<=':
                    $this->param['body']['query']['bool']['must'][]['range'][$key]['lte'] = $value;
                    break;
            }
        }
        return $this;
    }
    /**
     * 条件判断 或
     * @param $key
     * @param $compare
     * @param $value
     * @return $this
     */
    public function orwhere($key, $compare, $value, $separate = true) {
        switch ($compare) {
            case '=':
                if ($separate == true) {
                    $this->param['body']['query']['bool']['should'][] = [
                        'match' => [
                            $key => $value
                        ]
                    ];
                } else {
                    $this->param['body']['query']['bool']['should'] = [
                        'term' => [
                            $key => $value
                        ]
                    ];
                }
                break;
            case '>':
                $this->param['body']['query']['bool']['should'][]['range'][$key]['gt'] = $value;
                break;
            case '<':
                $this->param['body']['query']['bool']['should'][]['range'][$key]['lt'] = $value;
                break;
            case '>=':
                $this->param['body']['query']['bool']['should'][]['range'][$key]['gte'] = $value;
                break;
            case '<=':
                $this->param['body']['query']['bool']['should'][]['range'][$key]['lte'] = $value;
                break;
        }
        return $this;
    }
    /**
     * 判断条件  非
     * @param $key
     * @param $compare
     * @param $value
     * @return $this
     */
    public function wherenot($key, $compare, $value, $separate = true) {
        switch ($compare) {
            case '=':
                if ($separate == true) {
                    $this->param['body']['query']['bool']['must_not'][] = [
                        'match' => [
                            $key => $value
                        ]
                    ];
                } else {
                    $this->param['body']['query']['bool']['must_not'] = [
                        'term' => [
                            $key => $value
                        ]
                    ];
                }
                break;
            case '>':
                $this->param['body']['query']['bool']['must_not'][]['range'][$key]['gt'] = $value;
                break;
            case '<':
                $this->param['body']['query']['bool']['must_not'][]['range'][$key]['lt'] = $value;
                break;
            case '>=':
                $this->param['body']['query']['bool']['must_not'][]['range'][$key]['gte'] = $value;
                break;
            case '<=':
                $this->param['body']['query']['bool']['must_not'][]['range'][$key]['lte'] = $value;
                break;
        }
        return $this;
    }
    /**
     * 大于等于&&小于等于
     * @param $key
     * @param $lower
     * @param $upper
     * @return $this
     */
    public function between($key, $lower, $upper) {
        $this->param['body']['query']['range'] = [
            $key => [
                'from' => $lower,
                'to' => $upper,
                'include_lower' => true,
                'include_upper' => true
            ]
        ];
        return $this;
    }
    /**
     * 排序
     * @param $key
     * @param $type
     * @return $this
     */
    public function orderby($key, $type) {
        $type = strtolower($type);
        $this->param['body']['sort'][] = [
            $key => ['order' => $type]
        ];
        return $this;
    }
    /**
     * 分页部分
     * @param $limit
     * @return $this
     */
    public function limit($limit) {
        $this->param['body']['size'] = $limit;
        return $this;
    }
    public function offset($offset) {
        $this->param['body']['from'] = $offset;
        return $this;
    }
    /**
     * 获得一个文档
     * @param $id
     * @return array|string
     */
    public function find($id) {
        $this->param['id'] = $id;
        try {
            $response = $this->client->get($this->param);
        } catch (\Exception $e) {
            return 'Data not found.';
        }
        if ($response["found"] == false) {
            return 'Data not found.';
        }
        $data = $response['_source'];
        unset($this->param);
        return $data;
    }
    /**
     * future mode
     * @return array|string
     */
    public function get() {
        $this->param['client']['future'] = 'lazy';
        $future = [];
        try {
            $future = $this->client->get($this->param);
        } catch (\Exception $e) {
            return 'Data not found.';
        }
    }
    /**
     * 搜索
     * @return string
     */
    public function search() {
        try {
            $response = $this->client->search($this->param);
        } catch (\Exception $e) {
            return 'Data not found.';
        }
        if ($response['hits']['total']['value'] == 0) {
            return 'Data not found.';
        }
        $data['total'] = $response['hits']['total']['value'];
        foreach ($response['hits']['hits'] as $value) {
            $data['data'][] = $value['_source'];
        }
        unset($this->param);
        return $data;
    }
    /**
     * 单文档插入
     * @param array $data
     * @return string
     */
    public function add(array $data) {
        $this->param['body'] = $data;
        try {
            $response = $this->client->index($this->param);
        } catch (\Exception $e) {
            return 'Data insert fail,error:' . $e->getMessage();
        }
        unset($this->param);
        return 'Data insert success';
    }
    /**
     * 批量插入
     * @param array $data
     */
    public function bulk(array $data) {
        $this->param['body'] = [];
        $length = sizeof($data);
        for ($i = 1; $i <= $length; $i++) {
            $this->param['body'][] = $data[$i];
            if ($i % 200 == 0) {
                $response = $this->client->bulk($this->param);
                $this->param['body'] = [];
                unset($response);
            }
        }
        if (! empty($this->param['body'])) {
            $response = $this->client->bulk($this->param);
        }
    }
    /**
     * 更新
     * @param array $data
     * @return string
     */
    public function update(array $data) {
        $this->param['body'] = [
            'doc' => $data
        ];
        try {
            $this->client->update($this->param);
        } catch (\Exception $e) {
            return 'Data update fail,error:' . $e->getMessage();
        }
        unset($this->param);
        return 'Data update success';
    }
    /**
     * 更新或插入
     * @param array $data
     */
    public function upsert(array $data) {
        $this->param['body'] = [
            'upsert' => $data
        ];
        try {
            $this->client->update($this->param);
        } catch (\Exception $e) {
            return 'Data upsert fail,error' . $e->getMessage();
        }
        unset($this->param);
        return 'Data upsert success';
    }
    /**
     * 删除一个文档
     * @param $id
     * @return string
     */
    public function delete($id) {
        $this->param['id'] = $id;
        try {
            $this->client->delete($this->param);
        } catch (\Exception $e) {
            return 'Data delete fail,error:' . $e->getMessage();
        }
        unset($this->param);
        return 'Data delete success';
    }
}
$page = 1;
$limit = 3;
$offset = ($page - 1) * $limit;
$data = EsSearch::connection()->base('human')->limit($limit)->offset($offset)->search();
print_r($data);
