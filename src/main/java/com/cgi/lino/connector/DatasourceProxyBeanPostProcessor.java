// Copyright (C) 2021 CGI France
//
// This file is part of LINO.
//
// LINO is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// LINO is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with LINO.  If not, see <http://www.gnu.org/licenses/>.

package com.cgi.lino.connector;

import java.lang.reflect.Method;

import javax.sql.DataSource;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.ReflectionUtils;

import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

// Uncomment to debug sql queries batching
// @Component
public class DatasourceProxyBeanPostProcessor implements BeanPostProcessor {

	@Override
	public Object postProcessBeforeInitialization(final Object bean, final String beanName) throws BeansException {
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
		if (bean instanceof DataSource) {
			ProxyFactory factory = new ProxyFactory(bean);
			factory.setProxyTargetClass(true);
			factory.addAdvice(new ProxyDataSourceInterceptor((DataSource) bean));
			return factory.getProxy();
		}

		return bean;
	}

	private static class ProxyDataSourceInterceptor implements MethodInterceptor {

		private final DataSource dataSource;

		public ProxyDataSourceInterceptor(final DataSource dataSource) {
			this.dataSource = ProxyDataSourceBuilder.create(dataSource).name("Batch-Insert-Logger").asJson().countQuery().logQueryToSysOut().build();
		}

		@Override
		public Object invoke(final MethodInvocation invocation) throws Throwable {
			Method proxyMethod = ReflectionUtils.findMethod(dataSource.getClass(), invocation.getMethod().getName());
			if (proxyMethod != null) {
				return proxyMethod.invoke(dataSource, invocation.getArguments());
			}
			return invocation.proceed();
		}
	}
}
