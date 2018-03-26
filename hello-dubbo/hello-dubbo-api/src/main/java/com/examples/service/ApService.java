/**
 * 
 */
package com.examples.service;

import com.examples.entity.Ap;
import com.examples.exceptions.ApBizException;

/**
 * @author Administrator
 *
 */
public interface ApService {
	
	int  addAp(Ap ap) throws ApBizException;
}
