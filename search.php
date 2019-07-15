<?php
require 'vendor/autoload.php';
use Elasticsearch\ClientBuilder;
class EsSearch {
    private $client;
    private $param = [];
    public function __construct($host = ['http://127.0.0.1:9200']) {
        $this->client = ClientBuilder::create()->setHosts($host)->build();
    }
    /**
     * 设置搜索索引类型
     * @param $index
     * @param null $type
     * @return $this
     */
    public function base($index, $type = null) {
        $this->param = [
            'index' => $index,
            'type' => $type,
        ];
        return $this;
    }
    /**
     * 条件判断
     * @param $key
     * @param $value
     * @return $this
     */
    public function where($key, $compare, $value) {
        if ($key == 'id') {
            $this->param['id'] = $value;
        } else {
            switch ($compare) {
                case '=':
                    $this->param['body']['query']['bool']['must'] = [
                        'match' => [
                            $key => $value
                        ]
                    ];
                    break;
                case ">":
                    $this->param['body']['query']['bool']['must']['range'][$key]['gt'] = $value;
                    break;
                case "<":
                    $this->param['body']['query']['bool']['must']['range'][$key]['lt'] = $value;
                    break;
                case ">=":
                    $this->param['body']['query']['bool']['must']['range'][$key]['gte'] = $value;
                    break;
                case '<=':
                    $this->param['body']['query']['bool']['must']['range'][$key]['lte'] = $value;
                    break;
            }
        }
        return $this;
    }
    public function orwhere(){

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
        return $data;
    }
    public function get() {
        try {
            $response = $this->client->get($this->param);
        } catch (\Exception $e) {
            return 'Data not found.';
        }
        if ($response["found"] == false) {
            return 'Data not found.';
        }
        $data = $response;
        return $data;
    }
    /**
     * 搜索
     * @return string
     */
    public function search() {
        return $this->param;
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
            return 'Data insert fail';
        }
        return 'Data insert success';
    }
    public function bulk(array $data) {
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
            return 'Data update fail';
        }
        return 'Data update success';
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
            return 'fail' . $e;
        }
        return 'success';
    }
}
$es = new EsSearch();
$data = $es->base('human')->where('class', '>', 1)->where('age','>',20)->search();
print_r($data);