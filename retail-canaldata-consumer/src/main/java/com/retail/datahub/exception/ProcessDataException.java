package com.retail.datahub.exception;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.exception.ProcessDataException <br/>
 * date: 2016/4/25 17:44 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class ProcessDataException extends Exception{

    public ProcessDataException(String msg){
        super(msg);
    }

    public ProcessDataException(){
        super();
    }

    public ProcessDataException(Throwable cause) {
        super(cause);
    }
}
