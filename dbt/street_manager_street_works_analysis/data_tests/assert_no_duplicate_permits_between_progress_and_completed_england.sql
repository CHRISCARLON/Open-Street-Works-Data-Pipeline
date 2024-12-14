-- Test to ensure no permit reference numbers exist in both in-progress and completed works lists
select
    in_progress.permit_reference_number,
    in_progress.event_time as in_progress_event_time,
    in_progress.work_status_ref as in_progress_status,
    completed.event_time as completed_event_time,
    completed.work_status_ref as completed_status,
    'Permit Ref Number found in both in-progress and completed lists' as failure_reason
from {{ ref('in_progress_list_england') }} as in_progress
inner join {{ ref('completed_list_england') }} as completed
    on in_progress.permit_reference_number = completed.permit_reference_number