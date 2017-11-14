// Decompiled by DJ v3.12.12.101 Copyright 2016 Atanas Neshkov  Date: 11/13/2017 9:48:22 AM
// Home Page:  http://www.neshkov.com/dj.html - Check often for new version!
// Decompiler options: packimports(3) 
// Source File Name:   OutlookPatternextract.java

package com.hp.it.cx;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OutlookPatternextract
{

    public OutlookPatternextract()
    {
    }

    public static String extract(String content, String paterns)
    {
        String titleextracted = "";
        Pattern pattern = Pattern.compile(paterns);
        for(Matcher matcher = pattern.matcher(content); matcher.find();)
            titleextracted = matcher.group(1);

        return titleextracted;
    }
}
