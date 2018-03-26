package com.example.dubbo.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.examples.entity.Ap;
import com.examples.exceptions.ApBizException;
import com.examples.service.ApService;

public class ApServiceMock implements ApService {
	private static final  Logger LOG = LoggerFactory.getLogger(ApServiceStub.class);
	@Override
	public int addAp(Ap ap) throws ApBizException {
		//only mock implement
		LOG.info("the mock service. when RpcException throw");
		return -1;
	}

}
