package com.example.dubbo.provider;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.dubbo.rpc.RpcContext;
import com.examples.entity.Ap;
import com.examples.exceptions.ApBizException;
import com.examples.service.ApService;

public class ApServiceImpl implements ApService {
	
	
	private static Logger logger = LoggerFactory.getLogger(ApServiceImpl.class);
	
	@Override
	public int addAp(Ap ap) throws ApBizException {
		logger.info("the time is {}. the provider ip is {} ",new SimpleDateFormat("HH:mm:ss").format(new Date()),  RpcContext.getContext().getLocalAddress());
		logger.info("process the add Ap service, apMac is {} , apName is {}", ap.getApMac(), ap.getApName() );
		if("error".equals(ap.getApName())) {
			throw new ApBizException("the wrong name of AP");
		}
		return 1;
	}

}
