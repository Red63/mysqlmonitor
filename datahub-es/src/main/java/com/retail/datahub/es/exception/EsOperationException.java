package com.retail.datahub.es.exception;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.es.exception.EsOperationException <br/>
 * date: 2016/5/9 15:27 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class EsOperationException extends RuntimeException{

    public EsOperationException(){super();}

    public EsOperationException(String msg){
        super(msg);
    }

    public EsOperationException(String msg, Throwable cause){
        super(msg, cause);
    }
}
