## 테이블 업데이트에 필요한 기능을 제공하는 Repo

### 방법 1. 반복문으로 행 하나하나에 접근하여 udpate


### 방법 2. dummy_table 생성, 데이터 삽입 후 테이블 조인
1) join에 필요한 컬럼과 업데이트 대상 컬럼으로 구성된 딕셔너리를 input으로 받는다.

  - pandas dataframe에서 to_dict(orient='records') 함수 적용

```
# 예를 들어 어떤 pid의 'net_contents_prob'과 'reordered_PT_cnt' 컬럼 값을 업데이트 하려면 다음과 같은 json_input을 받는다. 

json_input = [{'pid': 12, 'net_contents_prob': '0.0', 'reordered_PT_cnt': 29},
 {'pid': 13,'net_contents_prob': '0.0', 'reordered_PT_cnt': 23},
 {'pid': 14,'net_contents_prob': '0.0', 'reordered_PT_cnt': 43},
 {'pid': 15,'net_contents_prob': '0.01', 'reordered_PT_cnt': 1},
 {'pid': 16,'net_contents_prob': '0.0', 'reordered_PT_cnt': 9},
 {'pid': 17,'net_contents_prob': '0.0', 'reordered_PT_cnt': 4}]
```

2) dummy_table에 값을 insert한다.

- 2-1) dummy table이 없고 `create_mode`가 0이면 stop

- 2-2) dummy table이 없고 `create_mode`가 1이면 새로 생성

- 2-3) dummy table이 있다면 delete, insert

3) dummy_table 과 원테이블을 join

## input 설명


- `acc` : DB 계정 정보
- `db` : 스키마 정보
- `table` : join할 대상 테이블
- `json_input` : join에 필요한 컬럼과 업데이트 대상 컬럼으로 구성된 딕셔너리
- `join_key` : join에 필요한 컬럼명
- `create_mode` : dummy_table이 없을 시 작업을 중단할지(=0), table을 새로 생성할지(=1) 정하는 옵션



## 적용 예시
```
# update
update_rows(acc=ai.vmdb_sql_info(),
                 db='yunsik_test',
                 table='test',
                 json_input=json_input,
                 join_key='pid')
                 
# join_update
bulk_update_rows(acc=ai.vmdb_sql_info(),
                 db='yunsik_test',
                 table='test',
                 json_input=json_input,
                 join_key='pid',
                 create_mode=1)

```
