package ru.psu.movs.trrp.socketmq;

import java.io.Serializable;

public class Row implements Serializable
{
    public boolean isEnd = false;
    public String d_name, description, p_name, o_name, phone, s_name;
    public int age;

    public Row()
    {
        isEnd = true;
    }

    public Row(String[] s)
    {
        d_name = s[0];
        description = s[1];
        p_name = s[2];
        o_name = s[3];
        phone = s[4];
        s_name = s[5];
        age = Integer.parseInt(s[6]);
    }
}
