{% macro get_cancellation_code_description(cancellation_code) -%}

    case {{ cancellation_code }}
        when 'A' then 'Carrier'
        when 'B' then 'Weather'
        when 'C' then 'NAS'
        when 'D' then 'Security'
        else 'Unknown'
    end

{%- endmacro %}