-- Daily calendar through 2047, including dates without a catalogue refresh.
with digits as (
  select 0 as d union all select 1 union all select 2 union all select 3 union all select 4
  union all select 5 union all select 6 union all select 7 union all select 8 union all select 9
), numbers as (
  select a.d + 10*b.d + 100*c.d + 1000*d.d as n
  from digits a cross join digits b cross join digits c cross join digits d
)
select cast({{ dbt.dateadd('day', 'n', "cast('2020-01-01' as date)") }} as date) as date_day
from numbers
