package com.example.dubbo.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.examples.entity.Ap;
import com.examples.exceptions.ApBizException;
import com.examples.service.ApService;

public class ApServiceStub implements ApService {
	
	private static final  Logger LOG = LoggerFactory.getLogger(ApServiceStub.class);
	
	private final ApService apService;
	
	public ApServiceStub(ApService theApService) {
		apService = theApService;
	}

	@Override
	public int addAp(Ap ap){
		
		try {
			
			if(StringUtils.isEmpty(ap.getApMac())) {
				LOG.error("the ap mac is empty. return");
				return 0;
			}
			return apService.addAp(ap);
		}catch (ApBizException e) {
			LOG.error("the remote service  throw exception ", e);
            return 0;
        }
	}

}
