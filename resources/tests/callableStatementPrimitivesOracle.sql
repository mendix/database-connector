declare
  l_date date := :1;
  l_long number(20,0) := :2;
begin
  :3 := TO_NUMBER(l_long);
  :4 := TO_CHAR(l_date, 'YYYY-MM-DD HH:mm:ss');
end;