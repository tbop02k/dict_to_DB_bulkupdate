## 테이블 업데이트에 필요한 기능을 제공하는 Repo

### 방법 1. 반복문으로 행 하나하나에 접근하여 udpate


### 방법 2. dummy_table 생성, 데이터 삽입 후 테이블 조인
- join에 필요한 컬럼(`list_dict_condition`), 업데이트 대상 컬럼(`list_dict_input`)으로 구성된 딕셔너리를 input으로 받는다.

```python
# 예를 들어 어떤 pid의 'net_contents_prob'과 'reordered_PT_cnt' 컬럼 값을 업데이트 하려면

list_dict_condition = [{'pid': 12},
                       {'pid': 13},
                       {'pid': 14},
                       {'pid': 15},
                       {'pid': 16},
                       {'pid': 17}]


list_dict_input = [{'net_contents_prob': '0.0', 'reordered_PT_cnt': 29},
                   {'net_contents_prob': '0.0', 'reordered_PT_cnt': 23},
                   {'net_contents_prob': '0.0', 'reordered_PT_cnt': 43},
                   {'net_contents_prob': '0.01', 'reordered_PT_cnt': 1},
                   {'net_contents_prob': '0.0', 'reordered_PT_cnt': 9},
                   {'net_contents_prob': '0.0', 'reordered_PT_cnt': 4}]

```
- dummy_table에 값을 insert한다.

    - dummy table이 없다면 create

    - dummy table이 있다면 drop, create, insert

    - `if_exists='replace'` 옵션이 이 기능을 수행함

- dummy_table 과 원테이블을 join
