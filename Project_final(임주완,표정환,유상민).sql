drop table if exists customer;
drop table if exists loan;

-- 권한 설정관련
mysql -u root -p
create database TeamProject;
use TeamProject;
GRANT ALL PRIVILEGES ON TeamProject.* TO 'SCOTT'@'%';
set global log_bin_trust_function_creators=on;


-- 연봉별 내부등급, 금리, 한도 나와있는 loan 테이블 작성
create table loan(
	credit_grade int primary key, 
	salmin int unsigned,
	salmax int unsigned,
	loan_interest_rate double,
	loan_interest_rate_limit int unsigned,
	check(loan_interest_rate between 0 and 0.2)
);

desc loan;

insert into loan values (10, 0,1000,0.2,2000);
insert into loan values (9, 1001,2000,0.18,4000);
insert into loan values (8, 2001,3000,0.16,6000);
insert into loan values (7, 3001,4000,0.14,8000);
insert into loan values (6, 4001,5000,0.12,10000);
insert into loan values (5, 5001,6000,0.10,12000);
insert into loan values (4, 6001,7000,0.08,14000);
insert into loan values (3, 7001,8000,0.06,16000);
insert into loan values (2, 8001,9000,0.04,18000);
insert into loan values (1, 9001,10000,0.04,20000);

-- 고객 정보 table 생성
create table customer(
	cnum int unsigned auto_increment primary key,
	cname varchar(10) not null,
	startday date,
	lastday date,
	sal int unsigned,
	loan_balance int unsigned default 0,
	is_overdue char(1) default 'N',
	credit_grade int,
	check(is_overdue in ('Y','N')),
	check(sal between 0 and 10000),
	foreign key(credit_grade) references loan(credit_grade)
);

desc customer;

insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("최종배","2003-01-31","2023-01-31",2490,1806,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("김기홍","2022-05-23","2023-05-23",6474,5907,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("김용재","2021-07-11","2026-07-11",4476,3001,"Y");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("원갑희","2000-08-01","2030-08-01",5183,8263,"Y");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("임인수","2013-04-04","2023-04-04",5816,6224,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("고은옥","2017-06-30","2022-06-30",8916,7509,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("윤성현","2021-06-22","2022-06-22",4945,5682,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("오세윤","2005-03-23","2025-03-23",516,1164,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("신종철","2010-10-13","2030-10-13",5209,1422,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("홍선의","2020-06-13","2025-06-23",7911,9386,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("유성수","2019-01-24","2024-01-24",4364,3379,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("우애자","2016-03-03","2026-03-03",9606,11676,"Y");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("김중광","2011-12-02","2031-12-02",4000,7011,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("이왕식","2022-09-28","2023-09-28",5729,2415,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("김효태","2003-07-11","2023-07-11",4403,230,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("이영송","2004-11-30","2024-11-30",8048,10019,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("황홍석","2010-02-07","2030-02-07",2533,2711,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("황영희","2022-03-19","2023-03-19",5195,4927,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("이수진","2008-04-11","2028-04-11",7261,11717,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("정재현","2009-09-09","2029-09-09",3371,3244,"N");
insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("김윤아","2010-01-09","2022-07-07",3328,3522,"N");

select * from customer;
select * from loan;

-- 1-1 update 하기위한 sub쿼리 작성, cnum과 credit_grade 가져와 cnum은 조인시 사용, credit_grade는 update할 내용
select customer.cnum, loan.credit_grade
from loan, customer 
where customer.sal between loan.salmin and loan.salmax;

-- 1-2 customer 테이블, 서브쿼리를 조인하여 update
UPDATE customer a 
join (select customer.cnum, loan.credit_grade
from loan, customer 
where customer.sal between loan.salmin and loan.salmax) b
on a.cnum = b.cnum
SET a.credit_grade = b.credit_grade;

-- 2-1 새 customer 테이블에 년별로 분류하는 파티션 등록, 외래키 존재로 불가
ALTER TABLE customer 
		partition by range (year(startday))
(
	PARTITION p_2000 VALUES LESS THAN (2010) ENGINE = INNODB, -- ~2009년까지의 대출자
	PARTITION p_2010 VALUES LESS THAN (2020) ENGINE = INNODB, -- 2010~2019년까지의 대출자
	PARTITION p_max VALUES LESS THAN MAXVALUE ENGINE = INNODB -- 2020년 이후의 대출자 / MAXVALUE가 추가되면 파티션을 추가할수 없게 된다.
	-- 파티션을 계속 추가하려면  위 문장 없이 계속 추가한다.
);


-- 2-2 년월별 파티션 등록
ALTER TABLE customer 
		partition by range (year(startday))
		subpartition by HASH ( month(startday))
		SUBPARTITIONS 12(
	PARTITION p_2000 VALUES LESS THAN (2010), -- ~2009년까지의 대출자
	PARTITION p_2010 VALUES LESS THAN (2020), -- 2010~2019년까지의 대출자
	PARTITION p_max VALUES LESS THAN MAXVALUE -- 2020년 이후의 대출자 / MAXVALUE가 추가되면 파티션을 추가할수 없게 된다.
	-- 파티션을 계속 추가하려면  위 문장 없이 계속 추가한다.
);



-- 3-1 해당 뷰(가상 테이블은) 고객이 자신의 정보를 조회할 때 보는 뷰
drop view if exists my_info;
create view my_info as 
	select cname as '고객 이름', startday as '대출 시작 일자',
		   lastday as '대출 상환 일자', is_overdue as '연체이력', loan_balance as '대출 잔액',
		   l.loan_interest_rate as '대출 금리', l.loan_interest_rate_limit as '대출 한도'
			from customer c, loan l
			where c.credit_grade = l.credit_grade;

select * from my_info;

-- 3-2 해당 뷰(가상 테이블)은 직원이 고객을 조회할 때 보는 뷰.
drop view if exists customer_info;

create view customer_info as 
	select cnum as '고객 번호', cname as '고객 이름', startday as '대출 시작 일자',
		   lastday as '대출 상환 일자', is_overdue as '연체이력', loan_balance as '대출 잔액',
		   l.credit_grade as '내부 등급', l.loan_interest_rate as '대출 금리', l.loan_interest_rate_limit as '대출 한도'
			from customer c, loan l
			where c.credit_grade = l.credit_grade;

select * from customer_info;


-- 4 원하는 등급별로 고객수 카운트
SELECT credit_grade as '신용등급', COUNT(*) as '인원 수'
from customer
WHERE credit_grade = 1
GROUP BY credit_grade;

SELECT credit_grade as '신용등급', COUNT(*) as '인원 수'
from customer
GROUP BY credit_grade
order by count(*) desc;

-- 5 인상 전 이자, 인상 후 이자
select c.cnum as 고객번호, c.cname as 고객이름 ,
round(l.loan_interest_rate*c.loan_balance/12,2) as "인상 전 이자(만원)",
round((l.loan_interest_rate+0.0025)*c.loan_balance/12,2) as "인상 후 이자(만원)"
from loan l, customer c
where l.credit_grade = c.credit_grade;

-- 6 연봉대비 대출잔액이 작은사람(마케팅)
SELECT cname as '고객성명', sal as '연봉', loan_balance as '대출잔액'
from customer c
WHERE loan_balance <= sal * 0.5;

-- 7 이번달 기준 다음달에 상환예정인 고객 리스트
SELECT cnum as ‘고객번호’ ,lastday as ‘만기일자‘, cname as ‘고객성명‘, loan_balance as ‘대출잔액’
from customer c3
where month(lastday) = MONTH(date_add(curdate(), interval 1 month)) and YEAR(lastday) = YEAR(CURDATE());

-- 8 만기 연장 대상: 연체한적 없고, 만기가 3개월 이하 남았고, 총 대출기간이 5년 미만인 사람
select cnum, cname, startday, lastday, is_overdue, DATE_ADD(lastday, INTERVAL 1 year) as "연장 후 만기"
from customer
where 
TIMESTAMPDIFF(MONTH, curdate(), lastday) <= 3 and
is_overdue = "N" and
TIMESTAMPDIFF(year, startday, lastday) < 5;

-- 9 고객별 현재 등급으로 가능한 대출한도(대출가능액 - 대출잔액) 계산 반환
select cnum, cname, sal, credit_grade, loan_balance, 
(cast(loan_interest_rate_limit as SIGNED) - CAST(loan_balance as SIGNED)) as "현재 한도(만원)"
from (select cnum, cname, sal, loan_balance, loan_interest_rate_limit, l.credit_grade
from customer c, loan l
where c.credit_grade = l.credit_grade) A;

-- 10 plsql trigger, customer table 새로운 데이터 insert, 연봉정보(sal) update시 자동으로 내부등급 업데이트
-- update 동시실행시 요류가 남

drop trigger auto_update;

CREATE trigger  auto_update
after insert
ON customer
FOR EACH row
BEGIN
declare v_credit_grade int;
set v_credit_grade = (select loan.credit_grade from loan, customer where cnum = new.cnum and new.sal between loan.salmin and loan.salmax);
update 
    customer
set
    credit_grade = v_credit_grade
where
    cnum = new.cnum;
END;

-- update를 실행하지 않고 새로 들어갈 값에 내부등급을 입력하는 방식으로 실행
drop trigger auto_up;

CREATE TRIGGER auto_up
BEFORE INSERT
ON customer
FOR EACH ROW
SET NEW.credit_grade = (select credit_grade from loan where new.sal between loan.salmin and loan.salmax);


insert into customer (cname, startday, lastday, sal, loan_balance, is_overdue) values ("최윤형","2020-01-31","2025-01-31",2222,5000,"N");
select * from customer;
