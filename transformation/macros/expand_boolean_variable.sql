{% macro expand_boolean_variable(variable) -%}

    case {{ variable }}
        when 0 then 'No'
        when 1 then 'Yes'
        else 'Unknown'
    end

{%- endmacro %}